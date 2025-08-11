# main.py
from fastapi import FastAPI, Request
from pydantic import BaseModel
import pandas as pd
from langchain_ollama import OllamaLLM
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import os, re, unicodedata, time, difflib
from dateutil import parser as dateutil_parser
from typing import Optional, List
import sqlite3
from contextlib import contextmanager

app = FastAPI()

# CORS — adapte si besoin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://10.4.100.35:3000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Config
# -----------------------
EXCEL_FILE = "CONSOMATION.xlsx"
PARQUET_FILE = "my_data.parquet"
SQLITE_DB = "consumption_data.db"
MODEL_NAME = "llama3.1:1b"
AGGREGATION_STRATEGY = "hybrid"
USE_DATABASE = True  # Set to False to use pandas approach

# -----------------------
# Models
# -----------------------
class Question(BaseModel):
    question: str
    mode: Optional[str] = None

# -----------------------
# Helpers: normalize, load
# -----------------------
def normalize_text(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip().upper()

def setup_sqlite_database():
    """Create SQLite database with optimized schema and indexes"""
    print("Setting up SQLite database...")
    
    # Load data from Excel/Parquet
    if os.path.exists(PARQUET_FILE):
        print("Loading from parquet...")
        df = pd.read_parquet(PARQUET_FILE)
    else:
        print("Loading from Excel...")
        df = pd.read_excel(EXCEL_FILE)
        df.to_parquet(PARQUET_FILE, index=False)

    # Clean and normalize data
    df['DATE_CONSO'] = pd.to_datetime(df['DATE_CONSO'], errors='coerce', dayfirst=True).dt.date
    df['FAMILLE_NORM'] = df['FAMILLE'].astype(str).apply(normalize_text)
    df['QTE'] = pd.to_numeric(df['QTE'].astype(str).str.replace(',', '.'), errors='coerce')
    df = df.dropna(subset=['DATE_CONSO', 'FAMILLE_NORM', 'QTE']).reset_index(drop=True)
    
    # Create SQLite database
    conn = sqlite3.connect(SQLITE_DB)
    
    # Create table with optimized schema
    conn.execute('''
        CREATE TABLE IF NOT EXISTS consumption (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_conso DATE NOT NULL,
            famille_norm TEXT NOT NULL,
            famille_original TEXT,
            qte REAL NOT NULL
        )
    ''')
    
    # Clear existing data
    conn.execute('DELETE FROM consumption')
    
    # Insert data
    for _, row in df.iterrows():
        conn.execute('''
            INSERT INTO consumption (date_conso, famille_norm, famille_original, qte)
            VALUES (?, ?, ?, ?)
        ''', (row['DATE_CONSO'], row['FAMILLE_NORM'], 
              row.get('FAMILLE', ''), row['QTE']))
    
    # Create indexes for fast queries
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date_famille ON consumption(date_conso, famille_norm)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_famille ON consumption(famille_norm)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON consumption(date_conso)')
    
    conn.commit()
    conn.close()
    
    print(f"Database setup complete. Total records: {len(df)}")
    return df['FAMILLE_NORM'].unique().tolist()

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    try:
        yield conn
    finally:
        conn.close()

def load_data_pandas() -> pd.DataFrame:
    """Original pandas approach - kept as fallback"""
    if os.path.exists(PARQUET_FILE):
        print("Loading from parquet...")
        df = pd.read_parquet(PARQUET_FILE)
    else:
        print("Loading from Excel...")
        df = pd.read_excel(EXCEL_FILE)
        df.to_parquet(PARQUET_FILE, index=False)

    df['DATE_CONSO'] = pd.to_datetime(df['DATE_CONSO'], errors='coerce', dayfirst=True).dt.date
    df['FAMILLE_NORM'] = df['FAMILLE'].astype(str).apply(normalize_text)
    df['QTE'] = pd.to_numeric(df['QTE'].astype(str).str.replace(',', '.'), errors='coerce')
    df = df.dropna(subset=['DATE_CONSO', 'FAMILLE_NORM', 'QTE']).reset_index(drop=True)
    return df

# Initialize data source
if USE_DATABASE:
    if not os.path.exists(SQLITE_DB):
        AVAILABLE_FAMILIES = sorted(setup_sqlite_database())
    else:
        # Get families from existing database
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT DISTINCT famille_norm FROM consumption ORDER BY famille_norm')
            AVAILABLE_FAMILIES = [row[0] for row in cursor.fetchall()]
    df_data = None  # Don't load into memory
else:
    df_data = load_data_pandas()
    AVAILABLE_FAMILIES = sorted(df_data['FAMILLE_NORM'].unique().tolist())

# Initialize LLM (guarded)
try:
    llm = OllamaLLM(model=MODEL_NAME, temperature=0.1)
except Exception as e:
    print("LLM init failed:", e)
    llm = None

# -----------------------
# Optimized Database Query Functions
# -----------------------
def query_consumption_data(start_date, end_date, famille):
    """Fast database query for consumption data"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_CONSO'] >= start_date) &
            (df_data['DATE_CONSO'] <= end_date) &
            (df_data['FAMILLE_NORM'] == famille)
        ].copy()
        return df_filtered
    
    # Use optimized SQL queries
    with get_db_connection() as conn:
        # Get aggregated data in one query
        cursor = conn.execute('''
            SELECT 
                SUM(qte) as total_sum,
                AVG(qte) as mean_val,
                MIN(qte) as min_val,
                MAX(qte) as max_val,
                COUNT(*) as count_val
            FROM consumption 
            WHERE date_conso BETWEEN ? AND ? 
            AND famille_norm = ?
        ''', (start_date, end_date, famille))
        
        agg_result = cursor.fetchone()
        
        # Get daily breakdown for ranges if needed
        daily_cursor = conn.execute('''
            SELECT 
                date_conso,
                SUM(qte) as daily_total,
                COUNT(*) as daily_count
            FROM consumption 
            WHERE date_conso BETWEEN ? AND ? 
            AND famille_norm = ?
            GROUP BY date_conso
            ORDER BY date_conso
        ''', (start_date, end_date, famille))
        
        daily_results = daily_cursor.fetchall()
        
        # Get sample rows (limited)
        rows_cursor = conn.execute('''
            SELECT date_conso, famille_norm, qte 
            FROM consumption 
            WHERE date_conso BETWEEN ? AND ? 
            AND famille_norm = ?
            ORDER BY date_conso
            LIMIT 100
        ''', (start_date, end_date, famille))
        
        sample_rows = rows_cursor.fetchall()
        
        return {
            'aggregates': {
                'sum': float(agg_result['total_sum'] or 0),
                'mean': float(agg_result['mean_val'] or 0),
                'min': float(agg_result['min_val'] or 0),
                'max': float(agg_result['max_val'] or 0),
                'count': int(agg_result['count_val'] or 0)
            },
            'daily_breakdown': [
                {
                    'date': row['date_conso'],
                    'total': float(row['daily_total']),
                    'entries': int(row['daily_count'])
                }
                for row in daily_results
            ],
            'sample_rows': [
                {
                    'DATE_CONSO': row['date_conso'],
                    'FAMILLE_NORM': row['famille_norm'],
                    'QTE': float(row['qte'])
                }
                for row in sample_rows
            ]
        }

# -----------------------
# Date parsing (kept same but faster)
# -----------------------
def parse_date_range_from_text(text: str):
    text = text.strip()
    
    range_patterns = [
        r'du\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à)\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'entre\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+et\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'de\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à|jusqu\'au|jusqu\s+au)\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à|-|jusqu\'au|jusqu\s+au)\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})'
    ]
    
    for pattern in range_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                d1 = dateutil_parser.parse(m.group(1), dayfirst=True).date()
                d2 = dateutil_parser.parse(m.group(2), dayfirst=True).date()
                return (min(d1, d2), max(d1, d2), 'range')
            except:
                continue
    
    single_date_patterns = [
        r'(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',
        r'(?:au|à)\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',
        r'pour\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
    ]
    
    for pattern in single_date_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                d = dateutil_parser.parse(m.group(1), dayfirst=True).date()
                return (d, d, 'single')
            except:
                continue
    
    dates = re.findall(r'\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}', text)
    parsed = []
    for ds in dates:
        try:
            parsed.append(dateutil_parser.parse(ds, dayfirst=True).date())
        except:
            continue
    
    if len(parsed) >= 2:
        return (min(parsed[0], parsed[1]), max(parsed[0], parsed[1]), 'range')
    elif len(parsed) == 1:
        return (parsed[0], parsed[0], 'single')
    
    return (None, None, None)

def detect_famille_in_text(text: str) -> Optional[str]:
    text_norm = normalize_text(text)

    variations = {
        normalize_text("MAIS"): normalize_text("MAIS"),
        normalize_text("MAÏS"): normalize_text("MAIS"),
        normalize_text("CORN"): normalize_text("MAIS"),
        normalize_text("BLE FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLED FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLÉ FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLÉ FOURAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("ORG"): normalize_text("ORGE"),
        normalize_text("SOJA"): normalize_text("GRAINES DE SOJA"),
    }
    
    for variant, standard in variations.items():
        if variant in text_norm:
            return standard

    for fam in AVAILABLE_FAMILIES:
        if fam == text_norm or fam in text_norm:
            return fam

    matches = difflib.get_close_matches(text_norm, AVAILABLE_FAMILIES, n=1, cutoff=0.8)
    if matches:
        return matches[0]
    
    words = [w for w in re.split(r'[\s,;:.!?()]+', text_norm) if len(w) > 2]
    for w in words:
        matches = difflib.get_close_matches(w, AVAILABLE_FAMILIES, n=1, cutoff=0.85)
        if matches:
            return matches[0]
    
    return None

def detect_math_operation(text: str):
    t = text.lower()
    
    number_patterns = [
        r'par\s+([-+]?\d+[.,]?\d*)',
        r'([-+]?\d+[.,]?\d*)\s*(?:fois|x|\*)',
        r'(?:diviser|divisé|divise)\s+par\s+([-+]?\d+[.,]?\d*)',
        r'(?:multiplier|multiplie|multiplié)\s+par\s+([-+]?\d+[.,]?\d*)',
        r'(?:ajouter|ajoute|ajouté)\s+([-+]?\d+[.,]?\d*)',
        r'(?:soustraire|soustrait|moins)\s+([-+]?\d+[.,]?\d*)'
    ]
    
    operand = None
    for pattern in number_patterns:
        match = re.search(pattern, t)
        if match:
            try:
                operand = float(match.group(1).replace(',', '.'))
                break
            except:
                continue
    
    operations = [
        (['divis', 'diviser', 'divisé', 'divise'], 'divide'),
        (['multipl', 'multiplié', 'multiplie', 'fois', 'x'], 'multiply'),
        (['ajout', 'ajoute', 'ajouter', 'plus', '+'], 'add'),
        (['soustrait', 'soustraire', 'moins', '-'], 'subtract'),
        (['somme', 'total', 'totaliser', 'additionner'], 'sum'),
        (['moyenn', 'moyenne', 'average'], 'average'),
        (['min', 'minim', 'minimum'], 'min'),
        (['max', 'maximum', 'maxim'], 'max'),
        (['nombre', 'count', 'compte', 'combien'], 'count')
    ]
    
    for keywords, op_type in operations:
        if any(keyword in t for keyword in keywords):
            return {'op': op_type, 'value': operand}
    
    return {'op': 'none', 'value': None}

def perform_operation(aggregates: dict, operation: dict):
    op = operation.get('op')
    v = operation.get('value')
    
    if op == 'none':
        return None, None
    
    if op == 'sum':
        return aggregates['sum'], f"Somme = {aggregates['sum']:.2f}"
    if op == 'average':
        return aggregates['mean'], f"Moyenne = {aggregates['mean']:.2f}"
    if op == 'min':
        return aggregates['min'], f"Minimum = {aggregates['min']:.2f}"
    if op == 'max':
        return aggregates['max'], f"Maximum = {aggregates['max']:.2f}"
    if op == 'count':
        return aggregates['count'], f"Nombre d'entrées = {aggregates['count']}"
    
    base = aggregates['sum']
    if v is None:
        return None, None
    
    try:
        if op == 'divide' and v != 0:
            result = base / v
            return result, f"{base:.2f} ÷ {v:.2f} = {result:.2f}"
        elif op == 'multiply':
            result = base * v
            return result, f"{base:.2f} × {v:.2f} = {result:.2f}"
        elif op == 'add':
            result = base + v
            return result, f"{base:.2f} + {v:.2f} = {result:.2f}"
        elif op == 'subtract':
            result = base - v
            return result, f"{base:.2f} - {v:.2f} = {result:.2f}"
    except Exception as e:
        print(f"Operation error: {e}")
        return None, None
    
    return None, None

# -----------------------
# Request logger
# -----------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    client = request.client.host if request.client else "unknown"
    start_time = time.time()
    print(f"--> {request.method} {request.url} from {client}")
    resp = await call_next(request)
    duration = round((time.time() - start_time) * 1000, 2)
    print(f"<-- {request.method} {request.url} {resp.status_code} ({duration}ms)")
    return resp

# -----------------------
# OPTIMIZED Endpoint
# -----------------------
@app.post("/query")
async def query_exact(q: Question):
    start_time = time.time()
    q_text: str = q.question or ""
    mode = (q.mode or AGGREGATION_STRATEGY or "hybrid").lower()
    debug_info: dict = {}

    print("\nQUERY START:", q_text)
    
    # Parse dates & family
    start_date, end_date, date_type = parse_date_range_from_text(q_text)
    famille = detect_famille_in_text(q_text)
    
    debug_info['normalized_question'] = normalize_text(q_text)
    debug_info['parsed_start'] = str(start_date) if start_date else None
    debug_info['parsed_end'] = str(end_date) if end_date else None
    debug_info['date_type'] = date_type
    debug_info['detected_family'] = famille

    if not start_date or not end_date:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Erreur: Date non trouvée. Formats acceptés: '03/06/2024', 'le 03/06/2024', 'au 03/06/2024', 'du 01/06/2024 au 30/06/2024'",
            "debug": debug_info,
            "execution_time": f"{execution_time} secondes"
        }

    if not famille:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Famille non trouvée. Familles disponibles: " + ", ".join(AVAILABLE_FAMILIES[:5]) + "...",
            "debug": debug_info,
            "available_families_sample": AVAILABLE_FAMILIES[:15],
            "execution_time": f"{execution_time} secondes"
        }

    # OPTIMIZED: Query data using fast database approach
    query_start = time.time()
    data_result = query_consumption_data(start_date, end_date, famille)
    query_time = round((time.time() - query_start) * 1000, 2)
    print(f"Database query took: {query_time}ms")

    if USE_DATABASE:
        aggregates = data_result['aggregates']
        rows_preview = data_result['sample_rows']
        daily_data = data_result['daily_breakdown']
        
        # Convert daily breakdown to expected format
        daily_breakdown = {}
        for item in daily_data:
            date_str = datetime.strptime(str(item['date']), '%Y-%m-%d').strftime('%d/%m/%Y')
            daily_breakdown[date_str] = {
                'total': round(item['total'], 2),
                'entries': item['entries']
            }
    else:
        # Pandas fallback
        df_range = data_result
        aggregates = {'sum': 0.0, 'mean': 0.0, 'min': 0.0, 'max': 0.0, 'count': 0}
        rows_preview = []
        daily_breakdown = {}
        
        if not df_range.empty:
            values_list = df_range['QTE'].astype(float).tolist()
            aggregates['sum'] = float(sum(values_list))
            aggregates['mean'] = float(pd.Series(values_list).mean())
            aggregates['min'] = float(min(values_list))
            aggregates['max'] = float(max(values_list))
            aggregates['count'] = int(len(values_list))
            
            for _, r in df_range.head(100).iterrows():
                rows_preview.append({
                    'DATE_CONSO': r['DATE_CONSO'].strftime("%Y-%m-%d"),
                    'FAMILLE_NORM': r['FAMILLE_NORM'],
                    'QTE': round(float(r['QTE']), 2)
                })
            
            if date_type == 'range':
                daily_summary = df_range.groupby('DATE_CONSO')['QTE'].agg(['sum', 'count']).reset_index()
                for _, row in daily_summary.iterrows():
                    date_str = row['DATE_CONSO'].strftime('%d/%m/%Y')
                    daily_breakdown[date_str] = {
                        'total': round(float(row['sum']), 2),
                        'entries': int(row['count'])
                    }

    # Detect requested operation
    operation = detect_math_operation(q_text)
    op_result, op_explanation = perform_operation(aggregates, operation)

    # Build simplified prompt (less verbose)
    llm_start = time.time()
    response_text = ""
    
    if llm is not None:
        try:
            # Simplified prompt to reduce LLM processing time
            if date_type == 'single':
                prompt = f"Consommation de {famille} le {start_date.strftime('%d/%m/%Y')}: {aggregates['sum']:.2f} unités ({aggregates['count']} entrées). Question: {q_text}. Réponds brièvement."
            else:
                prompt = f"Consommation de {famille} du {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}: {aggregates['sum']:.2f} unités ({aggregates['count']} entrées). Question: {q_text}. Réponds brièvement."
            
            if op_result is not None:
                prompt += f" Opération: {op_explanation}"
            
            response_text = llm.invoke(prompt).strip()
            llm_time = round((time.time() - llm_start) * 1000, 2)
            print(f"LLM processing took: {llm_time}ms")
        except Exception as e:
            print("LLM invoke error:", e)
            response_text = ""

    # Fast fallback if LLM fails
    if not response_text or len(response_text.strip()) < 10:
        if date_type == 'single':
            date_str = start_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                response_text = f"La consommation de {famille} le {date_str} est de {aggregates['sum']:.2f} unités"
                if aggregates['count'] > 1:
                    response_text += f" (sur {aggregates['count']} entrées)"
                response_text += "."
                
                if op_result is not None:
                    response_text += f" {op_explanation} = {op_result:.2f} unités."
            else:
                response_text = f"Aucune consommation de {famille} trouvée pour le {date_str}."
        else:
            start_str = start_date.strftime("%d/%m/%Y")
            end_str = end_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                response_text = f"La consommation totale de {famille} du {start_str} au {end_str} est de {aggregates['sum']:.2f} unités ({aggregates['count']} entrées)."
                
                if daily_breakdown and len(daily_breakdown) <= 10:  # Only show daily breakdown for reasonable ranges
                    response_text += "\n\nDétail par jour:"
                    for date_str, data in sorted(daily_breakdown.items(), key=lambda x: datetime.strptime(x[0], '%d/%m/%Y')):
                        entries_text = f" ({data['entries']} entrées)" if data['entries'] > 1 else ""
                        response_text += f"\n- {date_str}: {data['total']:.2f} unités{entries_text}"
                
                if op_result is not None:
                    response_text += f"\n\n{op_explanation} = {op_result:.2f} unités."
            else:
                response_text = f"Aucune consommation de {famille} trouvée entre le {start_str} et le {end_str}."

    execution_time = round(time.time() - start_time, 2)
    print(f"TOTAL EXECUTION TIME: {execution_time} seconds")

    return {
        "computed": {
            "sum": round(aggregates['sum'], 2),
            "mean": round(aggregates['mean'], 2),
            "min": round(aggregates['min'], 2),
            "max": round(aggregates['max'], 2),
            "count": aggregates['count'],
            "date_type": date_type,
            "daily_breakdown": daily_breakdown if date_type == 'range' else None,
            "operation_requested": operation,
            "operation_result": round(op_result, 2) if op_result is not None and isinstance(op_result, (int, float)) else None,
            "operation_explanation": op_explanation
        },
        "rows": rows_preview,
        "response": response_text,
        "debug": debug_info,
        "execution_time": f"{execution_time} secondes",
        "performance": {
            "database_query_ms": query_time if USE_DATABASE else None,
            "total_ms": round(execution_time * 1000, 2)
        }
    }

# -----------------------
# Validation & Health Check
# -----------------------
@app.get("/health")
async def health_check():
    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM consumption')
            count = cursor.fetchone()['count']
            return {"status": "healthy", "database": "sqlite", "records": count}
    else:
        return {"status": "healthy", "database": "pandas", "records": len(df_data)}

def validate_data():
    print("\nDATA VALIDATION:")
    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM consumption')
            count = cursor.fetchone()['count']
            cursor = conn.execute('SELECT MIN(date_conso), MAX(date_conso) FROM consumption')
            date_range = cursor.fetchone()
            print(f"Database mode - Total records: {count}")
            print(f"Date range: {date_range[0]} to {date_range[1]}")
    else:
        print(f"Pandas mode - Total records: {len(df_data)}")
        print(f"Date range: {df_data['DATE_CONSO'].min()} to {df_data['DATE_CONSO'].max()}")
    
    print(f"Available families: {len(AVAILABLE_FAMILIES)}")
    print(f"Families sample: {AVAILABLE_FAMILIES[:10]}")

validate_data()

if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on 0.0.0.0:8000 (Database mode: {USE_DATABASE})")
    uvicorn.run(app, host="0.0.0.0", port=8000)
    print("Server started.")