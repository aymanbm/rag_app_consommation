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
    """Get Oracle database connection with context management"""
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

# Example usage:
with get_connection() as conn:
    cursor = conn.cursor()
    # Your database operations here
    print("Connected to Oracle database")
    cursor.execute(
        "SELECT * FROM (SELECT ca.NUMERO, cde.POIDS_RECEPTIONNE, cde.SILO_DESTINATION, "
        "cde.PRODUIT, cde.LIBELLE_PRODUIT, pr.FAMILLE, cde.DATE_RECEPTION "
        "FROM ALFSAHELP.cdeachat ca "
        "INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero "
        "INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit) "
        "WHERE ROWNUM <= 1"
    )
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
