# main.py
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import time 
from Database.database import get_db_connection, initialize_data_source
from functions.query_execute import query_exact
from dotenv import load_dotenv

load_dotenv()

USE_DATABASE = os.getenv("USE_DATABASE") 

available_families, df_data = initialize_data_source()
app = FastAPI()

# CORS — adapte si besoin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://10.4.100.35:3000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# -----------------------
# Models
# -----------------------


# -----------------------
# Date parsing (kept same but faster)
# -----------------------



# -----------------------
# Request logger
# -----------------------
@app.middleware("http")
async def log_requests(request: Request, call_next):
    client = request.client.host if request.client else "unknown"
    start_time = time.time()
    print(f"--> {request.method} {request.url} from {client}")
    resp = await call_next(request)
    duration = round((time.time() - start_time) * 1000, 2)
    print(f"<-- {request.method} {request.url} {resp.status_code} ({duration}ms)")
    return resp

# -----------------------
# OPTIMIZED Endpoint
# -----------------------
@app.post("/query")
async def query_exact(q: Question):
    return await query_exact(q)
# -----------------------
# Validation & Health Check
# -----------------------
@app.get("/health")
async def health_check():
    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM consumption')
            count = cursor.fetchone()['count']
            return {"status": "healthy", "database": "sqlite", "records": count}
    else:
        return {"status": "healthy", "database": "pandas", "records": len(df_data)}

def validate_data():
    print("\nDATA VALIDATION:")
    if USE_DATABASE:
        with get_db_connection() as conn:
            cursor = conn.execute('SELECT COUNT(*) as count FROM consumption')
            count = cursor.fetchone()['count']
            cursor = conn.execute('SELECT MIN(date_conso), MAX(date_conso) FROM consumption')
            date_range = cursor.fetchone()
            print(f"Database mode - Total records: {count}")
            print(f"Date range: {date_range[0]} to {date_range[1]}")
    else:
        print(f"Pandas mode - Total records: {len(df_data)}")
        print(f"Date range: {df_data['DATE_CONSO'].min()} to {df_data['DATE_CONSO'].max()}")
    
    print(f"Available families: {len(available_families)}")
    print(f"Families sample: {available_families[:10]}")

validate_data()

if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on 0.0.0.0:8000 (Database mode: {USE_DATABASE})")
    uvicorn.run(app, host="0.0.0.0", port=8000)
    print("Server started.")