import cx_Oracle
import oracledb
import oracledb
import logging


logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(_name_)

oracle_host: str = "10.4.100.63"
oracle_port: int = 1521
oracle_service_name: str = "ORCLOET"
oracle_username: str = "SAHEL"
oracle_password: str = "*Sah12191"
oracle_schema: str = "ALFSAHELP"

def get_connection(self):
        """Get Oracle database connection with context management"""
        connection = None
        try:
            # Construct connection string with credentials
            dsn = f"{self.settings.oracle_host}:{self.settings.oracle_port}/{self.settings.oracle_service_name}"
            
            # Try cx_Oracle first, then fallback to oracledb
            try:
                connection = cx_Oracle.connect(
                    user=self.settings.oracle_username,
                    password=self.settings.oracle_password,
                    dsn=dsn
                )
                logger.info("Connected using cx_Oracle")
            except Exception as cx_error:
                logger.warning(f"cx_Oracle connection failed: {cx_error}")
                try:
                    connection = oracledb.connect(
                        user=self.settings.oracle_username,
                        password=self.settings.oracle_password,
                        dsn=dsn
                    )
                    logger.info("Connected using oracledb")
                except Exception as oracle_error:
                    logger.error(f"Both Oracle drivers failed: {oracle_error}")
                    raise
            
            yield connection
            
        except Exception as e:
            logger.error(f"Oracle connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Oracle connection closed")