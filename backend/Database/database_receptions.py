# database.py
import sqlite3
import os
import sys
from contextlib import contextmanager

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions.normalize_text import normalize_text
from backend.functions.reception.load_data_reception import load_data_pandas
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
        CREATE TABLE IF NOT EXISTS reception (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_recep DATE NOT NULL,
            famille_norm TEXT NOT NULL,
            famille_original TEXT,
            libelle_prod TEXT NOT NULL,
            produit TEXT NOT NULL,
            silo_dest TEXT NOT NULL,
            qte REAL NOT NULL
                 
        )
    ''')
    
    # Clear existing data
    conn.execute('DELETE FROM reception')
    
    # Insert data
    for _, row in df.iterrows():
        conn.execute('''
            INSERT INTO reception (date_recep, famille_norm, famille_original, libelle_prod, produit, silo_dest, qte)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (row['DATE_RECEP'], row['FAMILLE_NORM'], 
              row.get('FAMILLE', ''), row['LIBELLE_PROD'], row['PRODUIT'], row['SILO_DEST'], row['QTE']))
    
    # Create indexes for fast queries
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date_famille ON reception(date_recep, famille_norm)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_famille ON reception(famille_norm)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date ON reception(date_recep)')
    
    conn.commit()
    conn.close()
    
    print(f"Database setup complete. Total records: {len(df)}")
    return df['LIBELLE_PROD'].unique().tolist()

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = sqlite3.connect(SQLITE_DB)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    try:
        yield conn
    finally:
        conn.close()

def query_reception_data(start_date, end_date, labelle_prod, silo_dest, USE_DATABASE=USE_DATABASE):
    if labelle_prod != None:
        return query_labelle(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)
    elif silo_dest != None:
        return query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)

def query_labelle(start_date, end_date, labelle_prod, USE_DATABASE=USE_DATABASE):
    """Fast database query for reception data"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_RECEP'] >= start_date) &
            (df_data['DATE_RECEP'] <= end_date) &
            (df_data['LIBELLE_PROD'] == labelle_prod)
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
            FROM reception
            WHERE date_recep BETWEEN ? AND ?
            AND libelle_prod = ?
        ''', (start_date, end_date, labelle_prod))

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
                date_recep,
                SUM(qte) as daily_total,
                COUNT(*) as daily_count
            FROM reception
            WHERE date_recep BETWEEN ? AND ? 
            AND libelle_prod = ?
            GROUP BY date_recep
            ORDER BY date_recep
        ''', (start_date, end_date, labelle_prod))
        
        daily_results = daily_cursor.fetchall()
        
        # Get sample rows (limited)
        rows_cursor = conn.execute('''
            SELECT date_recep, libelle_prod, qte
            FROM reception
            WHERE date_recep BETWEEN ? AND ?
            AND libelle_prod = ?
            ORDER BY date_recep
            LIMIT 100
        ''', (start_date, end_date, labelle_prod))

        sample_rows = rows_cursor.fetchall()
        
        return {
            'aggregates': aggregates,
            'daily_breakdown': [
                {
                    'date': row['date_recep'],
                    'total': float(row['daily_total']),
                    'entries': int(row['daily_count'])
                }
                for row in daily_results
            ],
            'sample_rows': [
                {
                    'DATE_RECEP': row['date_recep'],
                    'LIBELLE_PROD': row['libelle_prod'],
                    'QTE': float(row['qte'])
                }
                for row in sample_rows
            ]
        }

def query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE):

    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_RECEP'] >= start_date) &
            (df_data['DATE_RECEP'] <= end_date) &
            (df_data['SILO_DEST'] == silo_dest)
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
            FROM reception
            WHERE date_recep BETWEEN ? AND ?
            AND silo_dest = ?
        ''', (start_date, end_date, silo_dest))

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
                date_recep, libelle_prod,
                SUM(qte) as daily_total,
                COUNT(*) as daily_count
            FROM reception
            WHERE date_recep BETWEEN ? AND ? 
            AND silo_dest = ?
            GROUP BY date_recep
            ORDER BY date_recep
        ''', (start_date, end_date, silo_dest))

        daily_results = daily_cursor.fetchall()
        
        # Get sample rows (limited)
        rows_cursor = conn.execute('''
            SELECT date_recep, libelle_prod, qte, silo_dest
            FROM reception
            WHERE date_recep BETWEEN ? AND ?
            AND silo_dest = ?
            ORDER BY date_recep
            LIMIT 100
        ''', (start_date, end_date, silo_dest))

        sample_rows = rows_cursor.fetchall()
        
        return {
            'aggregates': aggregates,
            'daily_breakdown': [
                {
                    'date': row['date_recep'],
                    'total': float(row['daily_total']),
                    'entries': int(row['daily_count'])
                }
                for row in daily_results
            ],
            'sample_rows': [
                {
                    'DATE_RECEP': row['date_recep'],
                    'LIBELLE_PROD': row['libelle_prod'],
                    'QTE': float(row['qte']),
                    'SILO_DEST': row['silo_dest']
                }
                for row in sample_rows
            ]
        }
# Initialize data source
def initialize_data_source(thing="LIBELLE_PROD",USE_DATABASE=USE_DATABASE, PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB):
    if USE_DATABASE:
        if not os.path.exists(SQLITE_DB):
            available_libelle_prods = sorted(setup_sqlite_database(PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB))
        else:
            # Get families from existing database
            with get_db_connection() as conn:
                cursor = conn.execute(f'SELECT DISTINCT {thing} FROM reception ORDER BY {thing}')
                available_libelle_prods = [row[0] for row in cursor.fetchall()]
        df_data = None  # Don't load into memory
    else:
        df_data = load_data_pandas()
        available_libelle_prods = sorted(df_data[thing].unique().tolist())
    return available_libelle_prods, df_data

available_libelle_prods, df_data = initialize_data_source()
available_silo_destinations, df_data = initialize_data_source(thing="SILO_DEST")
setup_sqlite_database(PARQUET_FILE=PARQUET_FILE, EXCEL_FILE=EXCEL_FILE, SQLITE_DB=SQLITE_DB)