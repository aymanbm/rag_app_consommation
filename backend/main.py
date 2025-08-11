import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import time 
from Database.database import initialize_data_source
from functions.query_execute import query_exact, Question
from dotenv import load_dotenv
from backend.Requests.validation import validate_data
from backend.Requests.health import check
load_dotenv()

# FIX: Properly convert environment variables to boolean
def str_to_bool(value):
    """Convert string environment variable to boolean"""
    if value is None:
        return False
    return value.lower() in ('true', '1', 'yes', 'on')

USE_DATABASE = str_to_bool(os.getenv("USE_DATABASE", "True"))  # Default to True
AGGREGATION_STRATEGY = os.getenv("AGGREGATION_STRATEGY", "hybrid")  # Default to hybrid

print(f"DEBUG: USE_DATABASE = {USE_DATABASE} (type: {type(USE_DATABASE)})")
print(f"DEBUG: AGGREGATION_STRATEGY = {AGGREGATION_STRATEGY}")

available_families, df_data = initialize_data_source(USE_DATABASE=USE_DATABASE)
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
# Endpoint
# -----------------------
@app.post("/query")
async def query_execution(q: Question):
    return await query_exact(q, USE_DATABASE=USE_DATABASE, AGGREGATION_STRATEGY=AGGREGATION_STRATEGY)

# -----------------------
# Validation & Health Check
# -----------------------
@app.get("/health")
async def health_check():
    return check(USE_DATABASE=USE_DATABASE)

validate_data(USE_DATABASE=USE_DATABASE, df_data=df_data, available_families=available_families)

if __name__ == "__main__":
    import uvicorn
    print(f"Starting server on 0.0.0.0:8000 (Database mode: {USE_DATABASE})")
    uvicorn.run(app, host="0.0.0.0", port=8000)
    print("Server started.")