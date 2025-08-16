from backend.Database.database_receptions import get_db_connection
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def check(USE_DATABASE=True, df_data=None):
    """Health check function for database connectivity and data availability"""
    
    if USE_DATABASE:
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get total count from Oracle tables
                cursor.execute('''
                    SELECT COUNT(*) as count 
                    FROM ALFSAHELP.cdeachat ca
                    INNER JOIN ALFSAHELP.cdeachligne cde ON cde.numero = ca.numero
                    INNER JOIN ALFSAHELP.produit pr ON pr.code = cde.produit
                ''')
                
                count_result = cursor.fetchone()
                count = count_result[0] if count_result else 0
                
                cursor.close()
                
                return {
                    "status": "healthy", 
                    "database": "oracle", 
                    "records": count,
                    "connection": "successful"
                }
                
        except Exception as e:
            return {
                "status": "error", 
                "database": "oracle", 
                "records": 0,
                "connection": "failed",
                "error": str(e)
            }
    else:
        try:
            if df_data is not None:
                return {
                    "status": "healthy", 
                    "database": "pandas", 
                    "records": len(df_data),
                    "connection": "in-memory"
                }
            else:
                return {
                    "status": "error", 
                    "database": "pandas", 
                    "records": 0,
                    "connection": "no-data",
                    "error": "DataFrame is None"
                }
        except Exception as e:
            return {
                "status": "error", 
                "database": "pandas", 
                "records": 0,
                "connection": "failed",
                "error": str(e)
            }

def detailed_check():
    """Detailed health check with additional metrics"""
    basic_health = check(USE_DATABASE=True)
    
    if basic_health["status"] == "healthy":
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Get additional metrics
                metrics = {}
                
                # Count unique products
                cursor.execute('''
                    SELECT COUNT(DISTINCT cde.LIBELLE_PRODUIT) 
                    FROM ALFSAHELP.cdeachligne cde 
                    WHERE cde.LIBELLE_PRODUIT IS NOT NULL
                ''')
                metrics['unique_products'] = cursor.fetchone()[0]
                
                # Count unique silos
                cursor.execute('''
                    SELECT COUNT(DISTINCT cde.SILO_DESTINATION) 
                    FROM ALFSAHELP.cdeachligne cde 
                    WHERE cde.SILO_DESTINATION IS NOT NULL
                ''')
                metrics['unique_silos'] = cursor.fetchone()[0]
                
                # Get date range
                cursor.execute('''
                    SELECT MIN(cde.DATE_RECEPTION), MAX(cde.DATE_RECEPTION)
                    FROM ALFSAHELP.cdeachligne cde 
                    WHERE cde.DATE_RECEPTION IS NOT NULL
                ''')
                date_result = cursor.fetchone()
                if date_result[0] and date_result[1]:
                    metrics['date_range'] = {
                        'from': date_result[0].strftime('%Y-%m-%d'),
                        'to': date_result[1].strftime('%Y-%m-%d')
                    }
                else:
                    metrics['date_range'] = {'from': None, 'to': None}
                
                # Recent activity (last 30 days)
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM ALFSAHELP.cdeachligne cde 
                    WHERE cde.DATE_RECEPTION >= SYSDATE - 30
                ''')
                metrics['recent_records'] = cursor.fetchone()[0]
                
                cursor.close()
                
                # Combine basic health with detailed metrics
                detailed_health = {**basic_health, "metrics": metrics}
                return detailed_health
                
        except Exception as e:
            basic_health["detailed_error"] = str(e)
            return basic_health
    
    return basic_health

if __name__ == "__main__":
    # Test both health check functions
    print("=== Basic Health Check ===")
    health = check()
    print(health)
    
    print("\n=== Detailed Health Check ===")
    detailed_health = detailed_check()
    print(detailed_health)