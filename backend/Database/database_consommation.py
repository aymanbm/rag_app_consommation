# database.py
import sqlite3
import pandas as pd
import os
import sys
from contextlib import contextmanager

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions.normalize_text import normalize_text
from backend.functions.consommation.load_data_consommation import load_data_pandas
from dotenv import load_dotenv

load_dotenv()

EXCEL_FILE = os.getenv("EXCEL_FILE")
PARQUET_FILE = os.getenv("PARQUET_FILE")
SQLITE_DB = os.getenv("SQLITE_DB")
USE_DATABASE = os.getenv("USE_DATABASE", "True").lower() == "true"
# -----------------------
# Helpers: normalize, load
# -----------------------


def setup_sqlite_database(PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB):
    """Create SQLite database with optimized schema and indexes"""
    print("Setting up SQLite database...")
    
    df = load_data_pandas()
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

def query_consumption_data(start_date, end_date, famille, USE_DATABASE=USE_DATABASE):
    """Fast database query for consumption data"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_CONSO'] >= start_date) &
            (df_data['DATE_CONSO'] <= end_date) &
            (df_data['FAMILLE_NORM'] == famille)
        ].copy()
        return df_filtered
    
    # Rest of your database code remains the same...
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

        aggregates = {
            'sum': float(agg_result['total_sum']) if agg_result['total_sum'] is not None else 0.0,
            'mean': float(agg_result['mean_val']) if agg_result['mean_val'] is not None else 0.0,
            'min': float(agg_result['min_val']) if agg_result['min_val'] is not None else 0.0,
            'max': float(agg_result['max_val']) if agg_result['max_val'] is not None else 0.0,
            'count': int(agg_result['count_val']) if agg_result['count_val'] is not None else 0
        }
        
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
            'aggregates': aggregates,
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

# Initialize data source
def initialize_data_source(USE_DATABASE=USE_DATABASE, PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB):
    if USE_DATABASE:
        if not os.path.exists(SQLITE_DB):
            available_families = sorted(setup_sqlite_database(PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB))
        else:
            # Get families from existing database
            with get_db_connection() as conn:
                cursor = conn.execute('SELECT DISTINCT famille_norm FROM consumption ORDER BY famille_norm')
                available_families = [row[0] for row in cursor.fetchall()]
        df_data = None  # Don't load into memory
    else:
        df_data = load_data_pandas()
        available_families = sorted(df_data['FAMILLE_NORM'].unique().tolist())
    return available_families, df_data

available_families, df_data = initialize_data_source()
setup_sqlite_database(PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB)