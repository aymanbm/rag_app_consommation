# debug_oracle.py - Simple test to verify Oracle connection and tables
import oracledb
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

oracle_host: str = "10.4.100.63"
oracle_port: int = 1521
oracle_service_name: str = "ORCLOET"
oracle_username: str = "SAHEL"
oracle_password: str = "*Sah12191"
oracle_schema: str = "ALFSAHELP"

@contextmanager
def get_connection():
    connection = None
    try:
        dsn = f"{oracle_host}:{oracle_port}/{oracle_service_name}"
        connection = oracledb.connect(
            user=oracle_username,
            password=oracle_password,
            dsn=dsn
        )
        logger.info("Connected to Oracle database")
        yield connection
    except Exception as e:
        logger.error(f"Oracle connection error: {e}")
        raise
    finally:
        if connection:
            connection.close()
            logger.info("Oracle connection closed")

def test_tables():
    """Test if tables exist and check their structure"""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Test 1: Check if tables exist
        print("\n=== Testing Table Existence ===")
        tables_to_check = ['ALFSAHELP.cdeachat', 'ALFSAHELP.cdeachligne', 'ALFSAHELP.produit']
        
        for table in tables_to_check:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE ROWNUM <= 1")
                count = cursor.fetchone()[0]
                print(f"✓ {table} exists (sample count: {count})")
            except Exception as e:
                print(f"✗ {table} error: {e}")
        
        # Test 2: Check column names in cdeachligne
        print("\n=== Checking cdeachligne Columns ===")
        try:
            cursor.execute("""
                SELECT column_name 
                FROM USER_TAB_COLUMNS 
                WHERE table_name = 'CDEACHLIGNE'
                ORDER BY column_name
            """)
            columns = [row[0] for row in cursor.fetchall()]
            print("Available columns in cdeachligne:")
            for col in columns:
                print(f"  - {col}")
        except Exception as e:
            print(f"Error getting column info: {e}")
        
        # Test 3: Sample data query
        print("\n=== Testing Sample Data Query ===")
        try:
            cursor.execute("""
                SELECT * FROM (
                    SELECT ca.NUMERO, cde.POIDS_RECEPTIONNE, cde.SILO_DESTINATION,
                    cde.PRODUIT, cde.LIBELLE_PRODUIT, pr.FAMILLE, cde.DATE_RECEPTION
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                ) WHERE ROWNUM <= 3
            """)
            
            rows = cursor.fetchall()
            print(f"✓ Sample query successful, got {len(rows)} rows")
            if rows:
                print("Sample row:", rows[0])
        except Exception as e:
            print(f"✗ Sample query error: {e}")
        
        # Test 4: Test distinct queries
        print("\n=== Testing Distinct Queries ===")
        
        queries = {
            "LIBELLE_PRODUIT": """
                SELECT DISTINCT cde.LIBELLE_PRODUIT 
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                WHERE cde.LIBELLE_PRODUIT IS NOT NULL AND ROWNUM <= 5
            """,
            "SILO_DESTINATION": """
                SELECT DISTINCT cde.SILO_DESTINATION 
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                WHERE cde.SILO_DESTINATION IS NOT NULL AND ROWNUM <= 5
            """,
            "FAMILLE": """
                SELECT DISTINCT pr.FAMILLE 
                FROM ALFSAHELP.cdeachat ca
                INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                WHERE pr.FAMILLE IS NOT NULL AND ROWNUM <= 5
            """
        }
        
        for field, query in queries.items():
            try:
                cursor.execute(query)
                results = cursor.fetchall()
                print(f"✓ {field}: got {len(results)} sample values")
                if results:
                    print(f"  Sample values: {[r[0] for r in results[:3]]}")
            except Exception as e:
                print(f"✗ {field} query error: {e}")
        
        cursor.close()

if __name__ == "__main__":
    try:
        test_tables()
    except Exception as e:
        print(f"Test failed: {e}")