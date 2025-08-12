import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from functions.normalize_text import normalize_text
from dotenv import load_dotenv

load_dotenv()

EXCEL_FILE = os.getenv("EXCEL_FILE")
PARQUET_FILE = os.getenv("PARQUET_FILE")

def load_data_pandas() -> pd.DataFrame:
    """Original pandas approach - kept as fallback"""
    if os.path.exists(PARQUET_FILE):
        print("Loading from parquet...")
        df = pd.read_parquet(PARQUET_FILE)
    else:
        print("Loading from CSV...")
        df = pd.read_csv(EXCEL_FILE)
        # df.to_parquet(PARQUET_FILE, index=False)
    print("hellllllllllllllllllllllllllllllo = ",df.head(1))
    df['DATE_RECEP'] = pd.to_datetime(df['DATE_RECEPTION'], errors='coerce', dayfirst=True).dt.date
    df['FAMILLE_NORM'] = df['FAMILLE'].astype(str).apply(normalize_text)
    df['LIBELLE_PROD'] = df['LIBELLE_PRODUIT'].astype(str).apply(normalize_text)
    df['PRODUIT'] = df['PRODUIT'].astype(str).apply(normalize_text)
    df['SILO_DEST'] = df['SILO_DESTINATION'].astype(str).apply(normalize_text)

    df['QTE'] = pd.to_numeric(df['POIDS_RECEPTIONNE'].astype(str).str.replace(',', '.'), errors='coerce')
    df = df.dropna(subset=['DATE_RECEP', 'FAMILLE_NORM', 'QTE', 'LIBELLE_PROD', 'PRODUIT', 'SILO_DEST']).reset_index(drop=True)
    return df