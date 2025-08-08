from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import pandas as pd
from langchain_ollama import OllamaLLM
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import os, re, unicodedata, time
from dateutil import parser as dateutil_parser

app = FastAPI()

# Allow your React origin on the LAN and localhost for dev.
# If you still get blocked during debugging, temporarily set allow_origins=["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://10.4.100.35:3000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Question(BaseModel):
    question: str

# Constants
EXCEL_FILE = "CONSOMATION.xlsx"
PARQUET_FILE = "my_data.parquet"
MODEL_NAME = "llama3.1:8b"

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

    df['DATE_CONSO'] = pd.to_datetime(df['DATE_CONSO'], errors='coerce', dayfirst=True).dt.date
    df['FAMILLE_NORM'] = df['FAMILLE'].apply(normalize_text)
    df['QTE'] = pd.to_numeric(df['QTE'].astype(str).str.replace(',', '.'), errors='coerce')
    df.set_index(['DATE_CONSO', 'FAMILLE_NORM'], inplace=True)
    return df.dropna()

# Initialize components
df_data = load_data()
FAMILIES = set(idx[1] for idx in df_data.index)
llm = OllamaLLM(model=MODEL_NAME, temperature=0)

def parse_date_from_text(text: str):
    m = re.search(r'\b\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}\b', text)
    if m:
        try:
            dt = dateutil_parser.parse(m.group(0), dayfirst=True, fuzzy=False)
            return dt.date()
        except:
            return None
    return None

def detect_famille_in_text(text: str) -> str:
    text_norm = normalize_text(text)
    if "MAIS" in text_norm:
        return "MAIS"
    variations = {"MAIS": ["MAIS", "MAÏS", "CORN"]}
    for standard, variants in variations.items():
        if any(variant.upper() in text_norm for variant in variants):
            return standard
    for fam in FAMILIES:
        if fam in text_norm:
            return fam
    return None

# Simple request logger
@app.middleware("http")
async def log_requests(request: Request, call_next):
    client = request.client.host if request.client else "unknown"
    print(f"--> {request.method} {request.url} from {client}")
    resp = await call_next(request)
    print(f"<-- {request.method} {request.url} {resp.status_code}")
    return resp

@app.post("/query")
async def query_exact(question: Question):
    start_time = time.time()
    formatted_date = None
    formatted_qte = None

    print(f"\nDEBUG INFO:")
    print(f"1. Original question: {question.question}")
    print(f"2. Available families: {sorted(list(FAMILIES))}")

    parsed_date = parse_date_from_text(question.question)
    print(f"3. Parsed date: {parsed_date}")

    if not parsed_date:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Erreur: Date non trouvée. Format attendu: JJ/MM/AAAA",
            "execution_time": f"{execution_time} secondes"
        }

    famille = detect_famille_in_text(question.question)
    print(f"4. Detected family: {famille}")

    if not famille:
        samples = ", ".join(sorted(list(FAMILIES))[:5])
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": f"Famille non trouvée. Familles disponibles: {samples}",
            "execution_time": f"{execution_time} secondes"
        }

    try:
        print(f"5. Looking up: Date={parsed_date}, Famille={famille}")
        qte = df_data.loc[(parsed_date, famille), 'QTE']
        print(f"8. Found quantity: {qte}")
        formatted_date = parsed_date.strftime("%d/%m/%Y")
        formatted_qte = f"{qte:.2f}".replace(".", ",")
        context = (
            f"Données trouvées:\n"
            f"- Famille: {famille}\n"
            f"- Date: {formatted_date}\n"
            f"- Quantité: {formatted_qte} unités"
        )
    except KeyError as e:
        print(f"9. Lookup failed: {str(e)}")
        formatted_date = parsed_date.strftime("%d/%m/%Y")
        context = f"Aucune donnée trouvée pour la famille {famille} à la date {formatted_date}."

    if formatted_qte:
        prompt = f"""
        Voici les informations demandées:
        {context}

        Réponse: La consommation de la famille {famille} le {formatted_date} est de {formatted_qte} unités."""
    else:
        prompt = f"""
        Voici les informations demandées:
        {context}

        Réponse: Aucune donnée n'est disponible pour la famille {famille} le {formatted_date}."""

    # Call LLM but guard against exceptions so server still returns JSON
    try:
        response_text = llm.invoke(prompt).strip()
    except Exception as e:
        print("LLM invoke error:", e)
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Erreur: échec du modèle LLM (voir logs serveur)",
            "execution_time": f"{execution_time} secondes"
        }

    execution_time = round(time.time() - start_time, 2)
    return {
        "response": response_text,
        "execution_time": f"{execution_time} secondes"
    }

# Data validation prints
def validate_data():
    print("\nDATA VALIDATION:")
    print(f"Total records: {len(df_data)}")
    print(f"Unique families: {sorted(list(FAMILIES))}")
    print(f"Date range: {min(idx[0] for idx in df_data.index)} to {max(idx[0] for idx in df_data.index)}")
    print(f"Sample of data:")
    print(df_data.head())

validate_data()

if __name__ == "__main__":
    import uvicorn
    print("Starting server on 0.0.0.0:8000 — accessible from LAN at http://10.4.100.35:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
