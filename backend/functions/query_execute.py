from functions.normalize_text import normalize_text
from functions.parse_date import parse_date_range_from_text
from functions.detections import detect_famille_in_text, detect_math_operation
import time
from Models.model import initialize_llm_model
from pydantic import BaseModel
from Database.database import initialize_data_source, query_consumption_data
import pandas as pd
from datetime import datetime
# from functions.operations import perform_operation
from typing import Optional

from functions.enhanced_operations import (
    detect_multiple_operations, 
    perform_multiple_operations, 
    generate_response_without_llm
)


available_families, df_data = initialize_data_source()



# -----------------------
# Models
# -----------------------

class Question(BaseModel):
    question: str
    mode: Optional[str] = None

llm = initialize_llm_model()

async def query_exact(q: Question,USE_DATABASE, AGGREGATION_STRATEGY):
    start_time = time.time()
    q_text: str = q.question or ""
    debug_info: dict = {}

    print("\nQUERY START:", q_text)
    
    # Parse dates & family
    start_date, end_date, date_type = parse_date_range_from_text(q_text)
    famille = detect_famille_in_text(q_text)
    
    debug_info['normalized_question'] = normalize_text(q_text)
    debug_info['parsed_start'] = str(start_date) if start_date else None
    debug_info['parsed_end'] = str(end_date) if end_date else None
    debug_info['date_type'] = date_type
    debug_info['detected_family'] = famille

    # Early validation
    if not start_date or not end_date:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Erreur: Date non trouvée. Formats acceptés: '03/06/2024', 'le 03/06/2024', 'au 03/06/2024', 'du 01/06/2024 au 30/06/2024'",
            "debug": debug_info,
            "execution_time": f"{execution_time} secondes"
        }

    if not famille:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Famille non trouvée. Formats acceptés: MAIS, electricité, gaz, eau, etc.",
            "debug": debug_info,
            "execution_time": f"{execution_time} secondes"
        }

    # Query data
    query_start = time.time()
    data_result = query_consumption_data(start_date, end_date, famille, USE_DATABASE)
    query_time = round((time.time() - query_start) * 1000, 2)
    print(f"Database query took: {query_time}ms")

    # Process data based on source
    if USE_DATABASE:
        aggregates = data_result['aggregates']
        rows_preview = data_result['sample_rows']
        daily_data = data_result['daily_breakdown']
        
        # Convert daily breakdown to expected format
        daily_breakdown = {}
        for item in daily_data:
            date_str = datetime.strptime(str(item['date']), '%Y-%m-%d').strftime('%d/%m/%Y')
            daily_breakdown[date_str] = {
                'total': round(item['total'], 2),
                'entries': item['entries']
            }
    else:
        # Pandas fallback
        df_range = data_result
        aggregates = {'sum': 0.0, 'mean': 0.0, 'min': 0.0, 'max': 0.0, 'count': 0}
        rows_preview = []
        daily_breakdown = {}
        
        if not df_range.empty:
            values_list = df_range['QTE'].astype(float).tolist()
            aggregates['sum'] = float(sum(values_list))
            aggregates['mean'] = float(pd.Series(values_list).mean())
            aggregates['min'] = float(min(values_list))
            aggregates['max'] = float(max(values_list))
            aggregates['count'] = int(len(values_list))
            
            for _, r in df_range.head(100).iterrows():
                rows_preview.append({
                    'DATE_CONSO': r['DATE_CONSO'].strftime("%Y-%m-%d"),
                    'FAMILLE_NORM': r['FAMILLE_NORM'],
                    'QTE': round(float(r['QTE']), 2)
                })
            
            if date_type == 'range':
                daily_summary = df_range.groupby('DATE_CONSO')['QTE'].agg(['sum', 'count']).reset_index()
                for _, row in daily_summary.iterrows():
                    date_str = row['DATE_CONSO'].strftime('%d/%m/%Y')
                    daily_breakdown[date_str] = {
                        'total': round(float(row['sum']), 2),
                        'entries': int(row['count'])
                    }

    # Detect operations (enhanced to handle multiple operations)
    operations_info = detect_multiple_operations(q_text)
    operations_result = perform_multiple_operations(aggregates, operations_info)
    
    print(f"Detected operations: {operations_info}")
    print(f"Operations result: {operations_result}")

    # Generate response without LLM
    response_text = generate_response_without_llm(
        famille, start_date, end_date, date_type, 
        aggregates, daily_breakdown, operations_result
    )

    execution_time = round(time.time() - start_time, 2)
    print(f"TOTAL EXECUTION TIME: {execution_time} seconds (NO LLM)")

    return {
        "computed": {
            "sum": round(aggregates['sum'], 2),
            "mean": round(aggregates['mean'], 2),
            "min": round(aggregates['min'], 2),
            "max": round(aggregates['max'], 2),
            "count": aggregates['count'],
            "date_type": date_type,
            "daily_breakdown": daily_breakdown if date_type == 'range' else None,
            "operations_detected": operations_info['operations'],
            "primary_operation": operations_info['primary_operation'],
            "multiple_operations": operations_info['is_multiple'],
            "operation_results": operations_result['results']
        },
        "rows": rows_preview,
        "response": response_text,
        "debug": debug_info,
        "execution_time": f"{execution_time} secondes",
        "performance": {
            "database_query_ms": query_time if USE_DATABASE else None,
            "total_ms": round(execution_time * 1000, 2),
            "used_llm": False
        }
    }

