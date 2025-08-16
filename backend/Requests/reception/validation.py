import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.Database.database_receptions import get_db_connection

def validate_data(USE_DATABASE=True, df_data=None, available_libelle=None):
    print("\nDATA VALIDATION:")
    
    if USE_DATABASE:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get total count
                cursor.execute('''
                    SELECT COUNT(*) as count 
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                ''')
                count_result = cursor.fetchone()
                count = count_result[0] if count_result else 0
                
                # Get date range
                cursor.execute('''
                    SELECT MIN(cde.DATE_RECEPTION) as min_date, MAX(cde.DATE_RECEPTION) as max_date 
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    WHERE cde.DATE_RECEPTION IS NOT NULL
                ''')
                date_result = cursor.fetchone()
                
                print(f"Database mode (Oracle) - Total records: {count}")
                
                if date_result and date_result[0] and date_result[1]:
                    min_date = date_result[0].strftime('%Y-%m-%d') if date_result[0] else 'N/A'
                    max_date = date_result[1].strftime('%Y-%m-%d') if date_result[1] else 'N/A'
                    print(f"Date range: {min_date} to {max_date}")
                else:
                    print("Date range: No dates found")
                
                # Additional Oracle-specific validation
                cursor.execute('''
                    SELECT COUNT(DISTINCT cde.LIBELLE_PRODUIT) as unique_products
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    WHERE cde.LIBELLE_PRODUIT IS NOT NULL
                ''')
                product_result = cursor.fetchone()
                unique_products = product_result[0] if product_result else 0
                print(f"Unique products in database: {unique_products}")
                
                cursor.execute('''
                    SELECT COUNT(DISTINCT cde.SILO_DESTINATION) as unique_silos
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    WHERE cde.SILO_DESTINATION IS NOT NULL
                ''')
                silo_result = cursor.fetchone()
                unique_silos = silo_result[0] if silo_result else 0
                print(f"Unique silos in database: {unique_silos}")
                
                cursor.close()
                
        except Exception as e:
            print(f"Error validating Oracle database: {e}")
            print("Database validation failed")
            
    else:
        # Pandas mode validation
        if df_data is not None:
            print(f"Pandas mode - Total records: {len(df_data)}")
            
            # Handle different possible column names for backward compatibility
            date_col = None
            for col in ['DATE_RECEPTION', 'DATE_RECEP']:
                if col in df_data.columns:
                    date_col = col
                    break
            
            if date_col:
                print(f"Date range: {df_data[date_col].min()} to {df_data[date_col].max()}")
            else:
                print("Date column not found in DataFrame")
        else:
            print("Pandas mode - No DataFrame provided")

    # Validate available libelle products
    if available_libelle is not None:
        print(f"Available libelle_prods: {len(available_libelle)}")
        if available_libelle:
            sample_size = min(10, len(available_libelle))
            print(f"libelle_prods sample: {available_libelle[:sample_size]}")
        else:
            print("No libelle products available")
    else:
        print("No available libelle list provided")

# Additional helper function for detailed Oracle validation
def detailed_oracle_validation():
    """Perform detailed validation of Oracle database structure and content"""
    print("\n=== DETAILED ORACLE VALIDATION ===")
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check table row counts
            tables_info = [
                ("ALFSAHELP.cdeachat", "Purchase Orders"),
                ("ALFSAHELP.cdeachligne", "Purchase Order Lines"),
                ("ALFSAHELP.produit", "Products")
            ]
            
            print("\nTable Statistics:")
            for table, description in tables_info:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  {description}: {count:,} records")
                except Exception as e:
                    print(f"  {description}: Error - {e}")
            
            # Check data quality
            print("\nData Quality Checks:")
            
            # Check for NULL values in key fields
            quality_checks = [
                ("NULL LIBELLE_PRODUIT", "SELECT COUNT(*) FROM ALFSAHELP.cdeachligne WHERE LIBELLE_PRODUIT IS NULL"),
                ("NULL SILO_DESTINATION", "SELECT COUNT(*) FROM ALFSAHELP.cdeachligne WHERE SILO_DESTINATION IS NULL"),
                ("NULL DATE_RECEPTION", "SELECT COUNT(*) FROM ALFSAHELP.cdeachligne WHERE DATE_RECEPTION IS NULL"),
                ("NULL POIDS_RECEPTIONNE", "SELECT COUNT(*) FROM ALFSAHELP.cdeachligne WHERE POIDS_RECEPTIONNE IS NULL")
            ]
            
            for check_name, query in quality_checks:
                try:
                    cursor.execute(query)
                    null_count = cursor.fetchone()[0]
                    print(f"  {check_name}: {null_count:,} records")
                except Exception as e:
                    print(f"  {check_name}: Error - {e}")
            
            # Sample recent data
            print("\nRecent Data Sample:")
            try:
                cursor.execute('''
                    SELECT * FROM (
                        SELECT cde.DATE_RECEPTION, cde.LIBELLE_PRODUIT, 
                               cde.POIDS_RECEPTIONNE, cde.SILO_DESTINATION
                        FROM ALFSAHELP.cdeachat ca
                        INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                        WHERE cde.DATE_RECEPTION IS NOT NULL
                        ORDER BY cde.DATE_RECEPTION DESC
                    ) WHERE ROWNUM <= 3
                ''')
                
                recent_rows = cursor.fetchall()
                for i, row in enumerate(recent_rows, 1):
                    date_str = row[0].strftime('%Y-%m-%d') if row[0] else 'N/A'
                    print(f"  {i}. {date_str} | {row[1]} | {row[2]} | {row[3]}")
                    
            except Exception as e:
                print(f"  Error getting recent data: {e}")
            
            cursor.close()
            
    except Exception as e:
        print(f"Detailed validation failed: {e}")

if __name__ == "__main__":
    # Test the validation functions
    validate_data(USE_DATABASE=True)
    detailed_oracle_validation()