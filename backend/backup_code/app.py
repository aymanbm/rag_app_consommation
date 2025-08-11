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
MODEL_NAME = "llama3.1:8b"
# "server" | "llm" | "hybrid"
AGGREGATION_STRATEGY = "hybrid"

# -----------------------
# Models
# -----------------------
class Question(BaseModel):
    question: str
    mode: Optional[str] = None  # override mode per-request

# -----------------------
# Helpers: normalize, load
# -----------------------
def normalize_text(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).strip().upper()

def load_data() -> pd.DataFrame:
    if os.path.exists(PARQUET_FILE):
        print("Loading from parquet...")
        df = pd.read_parquet(PARQUET_FILE)
    else:
        print("Loading from Excel...")
        df = pd.read_excel(EXCEL_FILE)
        df.to_parquet(PARQUET_FILE, index=False)

    # Normalize & parse
    df['DATE_CONSO'] = pd.to_datetime(df['DATE_CONSO'], errors='coerce', dayfirst=True).dt.date
    df['FAMILLE_NORM'] = df['FAMILLE'].astype(str).apply(normalize_text)
    df['QTE'] = pd.to_numeric(df['QTE'].astype(str).str.replace(',', '.'), errors='coerce')
    df = df.dropna(subset=['DATE_CONSO', 'FAMILLE_NORM', 'QTE']).reset_index(drop=True)
    return df

df_data = load_data()
AVAILABLE_FAMILIES = sorted(df_data['FAMILLE_NORM'].unique().tolist())

# Initialize LLM (guarded)
try:
    llm = OllamaLLM(model=MODEL_NAME, temperature=0)
except Exception as e:
    print("LLM init failed:", e)
    llm = None

# -----------------------
# IMPROVED Date parsing (handles single dates and ranges)
# -----------------------
def parse_date_range_from_text(text: str):
    text = text.strip()
    
    # 1) Range patterns: du X au Y (ou du X à Y)
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
    
    # 2) Single date patterns (more flexible)
    single_date_patterns = [
        r'(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',  # "le 03/06/2024" but not followed by range indicators
        r'(?:au|à)\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',  # "au 03/06/2024"
        r'pour\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',  # "pour le 03/06/2024"
    ]
    
    for pattern in single_date_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                d = dateutil_parser.parse(m.group(1), dayfirst=True).date()
                return (d, d, 'single')
            except:
                continue
    
    # 3) Fallback: find all dates and determine if range or single
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

# -----------------------
# Family detection (fuzzy) - IMPROVED
# -----------------------
def detect_famille_in_text(text: str) -> Optional[str]:
    text_norm = normalize_text(text)

    # Enhanced synonyms map
    variations = {
        normalize_text("MAIS"): normalize_text("MAIS"),
        normalize_text("MAÏS"): normalize_text("MAIS"),
        normalize_text("CORN"): normalize_text("MAIS"),
        normalize_text("BLE FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLED FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLÉ FOURRAGER"): normalize_text("BLE FOURRAGER"),
    }
    
    # Check variations first
    for variant, standard in variations.items():
        if variant in text_norm:
            return standard

    # Direct substring match with word boundaries
    for fam in AVAILABLE_FAMILIES:
        # Try exact match first
        if fam == text_norm:
            return fam
        # Then substring match
        if fam in text_norm:
            return fam

    # Fuzzy matching with different thresholds
    # First try with higher threshold for exact matches
    matches = difflib.get_close_matches(text_norm, AVAILABLE_FAMILIES, n=1, cutoff=0.8)
    if matches:
        return matches[0]
    
    # Then try individual words with high threshold
    words = [w for w in re.split(r'[\s,;:.!?()]+', text_norm) if len(w) > 2]
    for w in words:
        matches = difflib.get_close_matches(w, AVAILABLE_FAMILIES, n=1, cutoff=0.85)
        if matches:
            return matches[0]
    
    # Finally, lower threshold for partial matches
    matches = difflib.get_close_matches(text_norm, AVAILABLE_FAMILIES, n=1, cutoff=0.6)
    if matches:
        return matches[0]
    
    return None

# -----------------------
# IMPROVED Math operation detection
# -----------------------
def detect_math_operation(text: str):
    t = text.lower()
    
    # More flexible number extraction
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
    
    # Operation detection with more patterns
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
    
    # Aggregation operations
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
    
    # Mathematical operations on sum
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
    print(f"--> {request.method} {request.url} from {client}")
    resp = await call_next(request)
    print(f"<-- {request.method} {request.url} {resp.status_code}")
    return resp

# -----------------------
# IMPROVED Endpoint
# -----------------------
@app.post("/query")
async def query_exact(q: Question):
    start_time = time.time()
    q_text: str = q.question or ""
    mode = (q.mode or AGGREGATION_STRATEGY or "hybrid").lower()
    debug_info: dict = {}

    print("\nDEBUG INFO:")
    print("Original question:", q_text)
    print("Aggregation mode:", mode)
    print("Available families sample:", AVAILABLE_FAMILIES[:10])

    # IMPROVED: Parse dates & family with better detection
    start_date, end_date, date_type = parse_date_range_from_text(q_text)
    print("Parsed date range:", start_date, end_date, f"({date_type})")
    famille = detect_famille_in_text(q_text)
    print("Detected family:", famille)

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

    # Filter data
    df_range = df_data[
        (df_data['DATE_CONSO'] >= start_date) &
        (df_data['DATE_CONSO'] <= end_date) &
        (df_data['FAMILLE_NORM'] == famille)
    ].copy()

    # Compute aggregates
    aggregates = {'sum': 0.0, 'mean': 0.0, 'min': 0.0, 'max': 0.0, 'count': 0}
    values_list: List[float] = []
    rows_preview: List[dict] = []
    
    if not df_range.empty:
        values_list = df_range['QTE'].astype(float).tolist()
        aggregates['sum'] = float(sum(values_list))
        aggregates['mean'] = float(pd.Series(values_list).mean())
        aggregates['min'] = float(min(values_list))
        aggregates['max'] = float(max(values_list))
        aggregates['count'] = int(len(values_list))
        
        # Rows preview (limit)
        for _, r in df_range.head(100).iterrows():
            rows_preview.append({
                'DATE_CONSO': r['DATE_CONSO'].strftime("%Y-%m-%d"),
                'FAMILLE_NORM': r['FAMILLE_NORM'],
                'QTE': round(float(r['QTE']), 2)
            })

    # Detect requested operation
    operation = detect_math_operation(q_text)
    op_result, op_explanation = None, None
    if mode in ("server", "hybrid"):
        op_result, op_explanation = perform_operation(aggregates, operation)

    # Calculate daily breakdown for ranges
    daily_breakdown = {}
    if date_type == 'range' and not df_range.empty:
        daily_summary = df_range.groupby('DATE_CONSO')['QTE'].agg(['sum', 'count']).reset_index()
        for _, row in daily_summary.iterrows():
            date_str = row['DATE_CONSO'].strftime('%d/%m/%Y')
            daily_breakdown[date_str] = {
                'total': round(float(row['sum']), 2),
                'entries': int(row['count'])
            }

    # IMPROVED: Build better prompts
    prompt_parts = []
    
    # Context about the query
    date_context = f"une date spécifique ({start_date.strftime('%d/%m/%Y')})" if date_type == 'single' else f"une période du {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}"
    prompt_parts.append(f"Contexte: L'utilisateur demande des informations sur la consommation de {famille} pour {date_context}.")
    
    if mode == "llm":
        prompt_parts.append(f"Données brutes (QTE): {values_list}")
        prompt_parts.append("Analyse ces données et réponds clairement à la question de l'utilisateur en français.")
    else:
        # Provide calculated results
        prompt_parts.append("Résultats calculés:")
        prompt_parts.append(f"- Consommation totale: {aggregates['sum']:.2f} unités")
        prompt_parts.append(f"- Nombre d'entrées: {aggregates['count']}")
        if aggregates['count'] > 0:
            prompt_parts.append(f"- Moyenne: {aggregates['mean']:.2f} unités")
            prompt_parts.append(f"- Minimum: {aggregates['min']:.2f} unités")  
            prompt_parts.append(f"- Maximum: {aggregates['max']:.2f} unités")
        
        # Add daily breakdown for ranges
        if date_type == 'range' and daily_breakdown:
            prompt_parts.append("Détail par jour:")
            for date_str, data in sorted(daily_breakdown.items(), key=lambda x: datetime.strptime(x[0], '%d/%m/%Y')):
                entries_text = f" ({data['entries']} entrées)" if data['entries'] > 1 else ""
                prompt_parts.append(f"  - {date_str}: {data['total']:.2f} unités{entries_text}")
    
    if op_result is not None:
        prompt_parts.append(f"Opération demandée: {op_explanation}")
    
    prompt_parts.append(f"Question: {q_text}")
    if date_type == 'range' and daily_breakdown:
        prompt_parts.append("Inclus le détail par jour dans ta réponse de manière naturelle.")
    prompt_parts.append("Réponds de manière naturelle et directe en français. Ne mentionne pas les calculs techniques sauf si demandé.")

    prompt = "\n".join(prompt_parts)

    # Call LLM safely
    response_text = ""
    if llm is not None:
        try:
            response_text = llm.invoke(prompt).strip()
        except Exception as e:
            print("LLM invoke error:", e)
            response_text = ""

    # IMPROVED: Fallback with better formatting
    def is_refusal_or_empty(text: str) -> bool:
        if not text or len(text.strip()) < 10:
            return True
        t = text.lower()
        refusal_signs = [
            "ne peux pas", "ne peut pas", "i cannot", "i'm sorry",
            "je ne dispose", "je n'ai pas", "impossible",
            "cannot provide", "je ne peux", "unavailable"
        ]
        return any(sig in t for sig in refusal_signs)

    if llm is None or is_refusal_or_empty(response_text):
        # Build appropriate server response based on date type and operation
        if date_type == 'single':
            date_str = start_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                base_msg = f"La consommation de {famille} le {date_str} est de {aggregates['sum']:.2f} unités"
                if aggregates['count'] > 1:
                    base_msg += f" (répartie sur {aggregates['count']} entrées)"
                base_msg += "."
                
                if op_result is not None and isinstance(op_result, (int, float)):
                    response_text = f"{base_msg} {op_explanation} = {op_result:.2f} unités."
                else:
                    response_text = base_msg
            else:
                response_text = f"Aucune consommation de {famille} trouvée pour le {date_str}."
        else:
            # Range query
            start_str = start_date.strftime("%d/%m/%Y")
            end_str = end_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                base_msg = f"La consommation totale de {famille} du {start_str} au {end_str} est de {aggregates['sum']:.2f} unités"
                if aggregates['count'] > 1:
                    base_msg += f" (sur {aggregates['count']} entrées)"
                base_msg += "."
                
                # Add daily breakdown for ranges
                if daily_breakdown:
                    base_msg += "\n\nDétail par jour:"
                    for date_str, data in sorted(daily_breakdown.items(), key=lambda x: datetime.strptime(x[0], '%d/%m/%Y')):
                        entries_text = f" ({data['entries']} entrées)" if data['entries'] > 1 else ""
                        base_msg += f"\n- {date_str}: {data['total']:.2f} unités{entries_text}"
                
                if op_result is not None and isinstance(op_result, (int, float)):
                    response_text = f"{base_msg}\n\n{op_explanation} = {op_result:.2f} unités."
                else:
                    response_text = base_msg
            else:
                response_text = f"Aucune consommation de {famille} trouvée entre le {start_str} et le {end_str}."

    execution_time = round(time.time() - start_time, 2)

    # Final response
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
        "execution_time": f"{execution_time} secondes"
    }

# -----------------------
# Validation print (startup)
# -----------------------
def validate_data():
    print("\nDATA VALIDATION:")
    print(f"Total records: {len(df_data)}")
    print(f"Unique families sample: {AVAILABLE_FAMILIES[:10]}")
    print(f"Date range: {df_data['DATE_CONSO'].min()} to {df_data['DATE_CONSO'].max()}")
    print("Sample rows:")
    print(df_data.head().to_string())

validate_data()

if __name__ == "__main__":
    import uvicorn
    print("Starting server on 0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
    print("Server started.")