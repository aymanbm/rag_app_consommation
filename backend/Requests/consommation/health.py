from backend.Database.database_consommation import get_db_connection
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def check(USE_DATABASE=True, df_data=None):

    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM consumption')
            count = cursor.fetchone()['count']
            return {"status": "healthy", "database": "sqlite", "records": count}
    else:
        return {"status": "healthy", "database": "pandas", "records": len(df_data)}
