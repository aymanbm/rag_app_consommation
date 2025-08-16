# database.py
import oracledb
import os
import sys
import logging
from contextlib import contextmanager
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from functions.normalize_text import normalize_text
from functions.reception.load_data_reception import load_data_pandas

load_dotenv()

# Oracle connection configuration
oracle_host: str = "10.4.100.63"
oracle_port: int = 1521
oracle_service_name: str = "ORCLOET"
oracle_username: str = "SAHEL"
oracle_password: str = "*Sah12191"
oracle_schema: str = "ALFSAHELP"

# Environment variables (keeping for backward compatibility)
EXCEL_FILE = os.getenv("EXCEL_FILE")
PARQUET_FILE = os.getenv("PARQUET_FILE")
SQLITE_DB = os.getenv("SQLITE_DB")
USE_DATABASE = os.getenv("USE_DATABASE", "True").lower() == "true"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# Oracle Connection Manager
# -----------------------

@contextmanager
def get_db_connection():
    """Context manager for Oracle database connections"""
    connection = None
    try:
        # Construct connection string with credentials
        dsn = f"{oracle_host}:{oracle_port}/{oracle_service_name}"
        
        # Use oracledb (modern Oracle driver)
        connection = oracledb.connect(
            user=oracle_username,
            password=oracle_password,
            dsn=dsn
        )
        logger.info("Connected using oracledb")
        
        yield connection
        
    except Exception as e:
        logger.error(f"Oracle connection error: {e}")
        raise
    finally:
        if connection:
            connection.close()
            logger.info("Oracle connection closed")

# -----------------------
# Data Query Functions
# -----------------------

def query_reception_data(start_date, end_date, libelle_prod=None, silo_dest=None, USE_DATABASE=USE_DATABASE):
    """Main query dispatcher"""
    if libelle_prod is not None:               # ✅ Fixed condition
        return query_labelle(start_date, end_date, libelle_prod, USE_DATABASE=USE_DATABASE)  # ✅ Fixed parameter
    elif silo_dest is not None:
        return query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)
    else:
        return query_general(start_date, end_date, USE_DATABASE=USE_DATABASE)

def query_labelle(start_date, end_date, libelle_prod, USE_DATABASE=USE_DATABASE):
    """Query reception data by product label"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_RECEPTION'] >= start_date) &
            (df_data['DATE_RECEPTION'] <= end_date) &
            (df_data['LIBELLE_PRODUIT'] == libelle_prod)
        ].copy()
        return df_filtered
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get aggregated data in one query
        cursor.execute('''
            SELECT 
                SUM(cde.POIDS_RECEPTIONNE) as total_sum,
                AVG(cde.POIDS_RECEPTIONNE) as mean_val,
                MIN(cde.POIDS_RECEPTIONNE) as min_val,
                MAX(cde.POIDS_RECEPTIONNE) as max_val,
                COUNT(*) as count_val
            FROM ALFSAHELP.cdeachat ca
            INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
            INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
            WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND cde.LIBELLE_PRODUIT = :libelle_prod
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'libelle_prod': libelle_prod
        })

        agg_result = cursor.fetchone()

        aggregates = {
            'sum': float(agg_result[0]) if agg_result[0] is not None else 0.0,
            'mean': float(agg_result[1]) if agg_result[1] is not None else 0.0,
            'min': float(agg_result[2]) if agg_result[2] is not None else 0.0,
            'max': float(agg_result[3]) if agg_result[3] is not None else 0.0,
            'count': int(agg_result[4]) if agg_result[4] is not None else 0
        }
        
        # Get daily breakdown for ranges if needed
        cursor.execute('''
            SELECT 
                cde.DATE_RECEPTION,
                SUM(cde.POIDS_RECEPTIONNE) as daily_total,
                COUNT(*) as daily_count
            FROM ALFSAHELP.cdeachat ca
            INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
            INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
            WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND cde.LIBELLE_PRODUIT = :libelle_prod
            GROUP BY cde.DATE_RECEPTION
            ORDER BY cde.DATE_RECEPTION
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'libelle_prod': libelle_prod
        })
        
        daily_results = cursor.fetchall()
        
        # Get sample rows (limited)
        cursor.execute('''
            SELECT * FROM (
                SELECT cde.DATE_RECEPTION, cde.LIBELLE_PRODUIT, cde.POIDS_RECEPTIONNE
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
                AND cde.LIBELLE_PRODUIT = :libelle_prod
                ORDER BY cde.DATE_RECEPTION
            ) WHERE ROWNUM <= 100
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'libelle_prod': libelle_prod
        })

        sample_rows = cursor.fetchall()
        cursor.close()
        
        return {
            'aggregates': aggregates,
            'daily_breakdown': [
                {
                    'date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'total': float(row[1]) if row[1] else 0.0,
                    'entries': int(row[2]) if row[2] else 0
                }
                for row in daily_results
            ],
            'sample_rows': [
                {
                    'DATE_RECEPTION': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'LIBELLE_PRODUIT': row[1],
                    'POIDS_RECEPTIONNE': float(row[2]) if row[2] else 0.0
                }
                for row in sample_rows
            ]
        }

def query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE):
    """Query reception data by silo destination"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_RECEPTION'] >= start_date) &
            (df_data['DATE_RECEPTION'] <= end_date) &
            (df_data['SILO_DESTINATION'] == silo_dest)
        ].copy()
        return df_filtered
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        # Get aggregated data in one query
        cursor.execute('''
            SELECT 
                SUM(cde.POIDS_RECEPTIONNE) as total_sum,
                AVG(cde.POIDS_RECEPTIONNE) as mean_val,
                MIN(cde.POIDS_RECEPTIONNE) as min_val,
                MAX(cde.POIDS_RECEPTIONNE) as max_val,
                COUNT(*) as count_val
            FROM ALFSAHELP.cdeachat ca
            INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
            INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
            WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND cde.SILO_DESTINATION = :silo_dest
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'silo_dest': silo_dest
        })

        agg_result = cursor.fetchone()

        aggregates = {
            'sum': float(agg_result[0]) if agg_result[0] is not None else 0.0,
            'mean': float(agg_result[1]) if agg_result[1] is not None else 0.0,
            'min': float(agg_result[2]) if agg_result[2] is not None else 0.0,
            'max': float(agg_result[3]) if agg_result[3] is not None else 0.0,
            'count': int(agg_result[4]) if agg_result[4] is not None else 0
        }
        
        # Get daily breakdown for ranges if needed
        cursor.execute('''
            SELECT 
                cde.DATE_RECEPTION, cde.LIBELLE_PRODUIT,
                SUM(cde.POIDS_RECEPTIONNE) as daily_total,
                COUNT(*) as daily_count
            FROM ALFSAHELP.cdeachat ca
            INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
            INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
            WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
            AND cde.SILO_DESTINATION = :silo_dest
            GROUP BY cde.DATE_RECEPTION, cde.LIBELLE_PRODUIT
            ORDER BY cde.DATE_RECEPTION
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'silo_dest': silo_dest
        })

        daily_results = cursor.fetchall()
        
        # Get sample rows (limited)
        cursor.execute('''
            SELECT * FROM (
                SELECT cde.DATE_RECEPTION, cde.LIBELLE_PRODUIT, cde.POIDS_RECEPTIONNE, cde.SILO_DESTINATION
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
                AND cde.SILO_DESTINATION = :silo_dest
                ORDER BY cde.DATE_RECEPTION
            ) WHERE ROWNUM <= 100
        ''', {
            'start_date': start_date, 
            'end_date': end_date, 
            'silo_dest': silo_dest
        })

        sample_rows = cursor.fetchall()
        cursor.close()
        
        return {
            'aggregates': aggregates,
            'daily_breakdown': [
                {
                    'date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'libelle_prod': row[1],
                    'total': float(row[2]) if row[2] else 0.0,
                    'entries': int(row[3]) if row[3] else 0
                }
                for row in daily_results
            ],
            'sample_rows': [
                {
                    'DATE_RECEPTION': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'LIBELLE_PRODUIT': row[1],
                    'POIDS_RECEPTIONNE': float(row[2]) if row[2] else 0.0,
                    'SILO_DESTINATION': row[3]
                }
                for row in sample_rows
            ]
        }

def query_general(start_date, end_date, USE_DATABASE=USE_DATABASE):
    """General query for date range without specific filters"""
    if not USE_DATABASE:
        # Fallback to pandas
        df_filtered = df_data[
            (df_data['DATE_RECEPTION'] >= start_date) &
            (df_data['DATE_RECEPTION'] <= end_date)
        ].copy()
        return df_filtered
    
    with get_db_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM (
                SELECT ca.NUMERO, cde.POIDS_RECEPTIONNE, cde.SILO_DESTINATION,
                cde.PRODUIT, cde.LIBELLE_PRODUIT, pr.FAMILLE, cde.DATE_RECEPTION
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                WHERE cde.DATE_RECEPTION BETWEEN TO_DATE(:start_date, 'YYYY-MM-DD') AND TO_DATE(:end_date, 'YYYY-MM-DD')
                ORDER BY cde.DATE_RECEPTION
            ) WHERE ROWNUM <= 1000
        ''', {
            'start_date': start_date, 
            'end_date': end_date
        })
        
        rows = cursor.fetchall()
        cursor.close()
        
        return [
            {
                'NUMERO': row[0],
                'POIDS_RECEPTIONNE': float(row[1]) if row[1] else 0.0,
                'SILO_DESTINATION': row[2],
                'PRODUIT': row[3],
                'LIBELLE_PRODUIT': row[4],
                'FAMILLE': row[5],
                'DATE_RECEPTION': row[6].strftime('%Y-%m-%d') if row[6] else None
            }
            for row in rows
        ]

# -----------------------
# Data Initialization
# -----------------------

def initialize_data_source(thing="LIBELLE_PRODUIT", USE_DATABASE=USE_DATABASE):
    """Initialize data source and get unique values"""
    if USE_DATABASE:
        # Get unique values from Oracle database
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            try:
                if thing == "LIBELLE_PRODUIT":
                    cursor.execute('''
                        SELECT DISTINCT cde.LIBELLE_PRODUIT 
                        FROM ALFSAHELP.cdeachat ca
                        INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                        WHERE cde.LIBELLE_PRODUIT IS NOT NULL
                        ORDER BY cde.LIBELLE_PRODUIT
                    ''')
                elif thing == "SILO_DESTINATION" or thing == "SILO_DEST":
                    cursor.execute('''
                        SELECT DISTINCT cde.SILO_DESTINATION 
                        FROM ALFSAHELP.cdeachat ca
                        INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                        WHERE cde.SILO_DESTINATION IS NOT NULL
                        ORDER BY cde.SILO_DESTINATION
                    ''')
                elif thing == "FAMILLE":
                    cursor.execute('''
                        SELECT DISTINCT pr.FAMILLE 
                        FROM ALFSAHELP.cdeachat ca
                        INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                        INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                        WHERE pr.FAMILLE IS NOT NULL
                        ORDER BY pr.FAMILLE
                    ''')
                else:
                    logger.warning(f"Unknown field requested: {thing}")
                    available_items = []
                    cursor.close()
                    return available_items, None
                
                # Fetch results
                results = cursor.fetchall()
                available_items = [row[0] for row in results if row[0] is not None]
                logger.info(f"Found {len(available_items)} unique values for {thing}")
                
            except Exception as e:
                logger.error(f"Error executing query for {thing}: {e}")
                available_items = []
            finally:
                cursor.close()
        
        df_data = None  # Don't load into memory when using database
    else:
        # Fallback to pandas
        try:
            df_data = load_data_pandas()
            # Handle different column name variations
            column_mapping = {
                "SILO_DEST": "SILO_DESTINATION",
                "LIBELLE_PROD": "LIBELLE_PRODUIT"
            }
            actual_column = column_mapping.get(thing, thing)
            
            if actual_column in df_data.columns:
                available_items = sorted(df_data[actual_column].dropna().unique().tolist())
            else:
                logger.warning(f"Column {actual_column} not found in DataFrame")
                available_items = []
        except Exception as e:
            logger.error(f"Error loading pandas data: {e}")
            available_items = []
            df_data = None
    
    return available_items, df_data

# Initialize available options
try:
    logger.info("Initializing data sources...")
    
    # Initialize each data source separately to isolate errors
    try:
        available_libelle_prods, df_data = initialize_data_source("LIBELLE_PRODUIT")
        logger.info(f"Loaded {len(available_libelle_prods)} products")
    except Exception as e:
        logger.error(f"Error loading products: {e}")
        available_libelle_prods = []
    
    try:
        available_silo_destinations, _ = initialize_data_source("SILO_DESTINATION")
        logger.info(f"Loaded {len(available_silo_destinations)} silos")
    except Exception as e:
        logger.error(f"Error loading silos: {e}")
        available_silo_destinations = []
    
    try:
        available_families, _ = initialize_data_source("FAMILLE")
        logger.info(f"Loaded {len(available_families)} families")
    except Exception as e:
        logger.error(f"Error loading families: {e}")
        available_families = []
    
    logger.info(f"Initialization complete: {len(available_libelle_prods)} products, "
                f"{len(available_silo_destinations)} silos, "
                f"{len(available_families)} families")
                
except Exception as e:
    logger.error(f"Critical error during initialization: {e}")
    # Fallback to empty lists
    available_libelle_prods = []
    available_silo_destinations = []
    available_families = []
    df_data = None

# Legacy function for backward compatibility (now removed as it's Oracle-based)
def setup_sqlite_database(*args, **kwargs):
    """Legacy function - no longer needed with Oracle database"""
    logger.warning("setup_sqlite_database is deprecated when using Oracle database")
    return []