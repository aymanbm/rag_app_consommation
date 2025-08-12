import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.Database.database_receptions import get_db_connection

def validate_data(USE_DATABASE=True, df_data=None, available_libelle=None):
    print("\nDATA VALIDATION:")
    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM reception')
            count = cursor.fetchone()['count']
            cursor = conn.execute('SELECT MIN(date_recep), MAX(date_recep) FROM reception')
            date_range = cursor.fetchone()
            print(f"Database mode - Total records: {count}")
            print(f"Date range: {date_range[0]} to {date_range[1]}")
    else:
        print(f"Pandas mode - Total records: {len(df_data)}")
        print(f"Date range: {df_data['DATE_RECEP'].min()} to {df_data['DATE_RECEP'].max()}")

    print(f"Available libelle_prods: {len(available_libelle)}")
    print(f"libelle_prods sample: {available_libelle[:10]}")
