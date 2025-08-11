from functions.normalize_text import normalize_text
from functions.parse_date import parse_date_range_from_text
from functions.detections import detect_famille_in_text, detect_math_operation
import time
import os
from typing import Optional
from dotenv import load_dotenv
from Models.model import initialize_llm_model
from pydantic import BaseModel
from Database.database import initialize_data_source, query_consumption_data
import pandas as pd
import datetime
from functions.operations import perform_operation
load_dotenv()

available_families, df_data = initialize_data_source()

AGGREGATION_STRATEGY = os.getenv("AGGREGATION_STRATEGY")

class Question(BaseModel):
    question: str
    mode: Optional[str] = None

llm = initialize_llm_model()

async def query_exact(q: Question):
    start_time = time.time()
    q_text: str = q.question or ""
    mode = (q.mode or AGGREGATION_STRATEGY or "hybrid").lower()
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
            "response": "Famille non trouvée. Familles disponibles: " + ", ".join(available_families[:5]) + "...",
            "debug": debug_info,
            "available_families_sample": available_families[:15],
            "execution_time": f"{execution_time} secondes"
        }

    # OPTIMIZED: Query data using fast database approach
    query_start = time.time()
    data_result = query_consumption_data(start_date, end_date, famille)
    query_time = round((time.time() - query_start) * 1000, 2)
    print(f"Database query took: {query_time}ms")

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

    # Detect requested operation
    operation = detect_math_operation(q_text)
    op_result, op_explanation = perform_operation(aggregates, operation)

    # Build simplified prompt (less verbose)
    llm_start = time.time()
    response_text = ""
    
    if llm is not None:
        try:
            # Simplified prompt to reduce LLM processing time
            if date_type == 'single':
                prompt = f"Consommation de {famille} le {start_date.strftime('%d/%m/%Y')}: {aggregates['sum']:.2f} unités ({aggregates['count']} entrées). Question: {q_text}. Réponds brièvement."
            else:
                prompt = f"Consommation de {famille} du {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}: {aggregates['sum']:.2f} unités ({aggregates['count']} entrées). Question: {q_text}. Réponds brièvement."
            
            if op_result is not None:
                prompt += f" Opération: {op_explanation}"
            
            response_text = llm.invoke(prompt).strip()
            llm_time = round((time.time() - llm_start) * 1000, 2)
            print(f"LLM processing took: {llm_time}ms")
        except Exception as e:
            print("LLM invoke error:", e)
            response_text = ""

    # Fast fallback if LLM fails
    if not response_text or len(response_text.strip()) < 10:
        if date_type == 'single':
            date_str = start_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                response_text = f"La consommation de {famille} le {date_str} est de {aggregates['sum']:.2f} unités"
                if aggregates['count'] > 1:
                    response_text += f" (sur {aggregates['count']} entrées)"
                response_text += "."
                
                if op_result is not None:
                    response_text += f" {op_explanation} = {op_result:.2f} unités."
            else:
                response_text = f"Aucune consommation de {famille} trouvée pour le {date_str}."
        else:
            start_str = start_date.strftime("%d/%m/%Y")
            end_str = end_date.strftime("%d/%m/%Y")
            if aggregates['count'] > 0:
                response_text = f"La consommation totale de {famille} du {start_str} au {end_str} est de {aggregates['sum']:.2f} unités ({aggregates['count']} entrées)."
                
                if daily_breakdown and len(daily_breakdown) <= 10:  # Only show daily breakdown for reasonable ranges
                    response_text += "\n\nDétail par jour:"
                    for date_str, data in sorted(daily_breakdown.items(), key=lambda x: datetime.strptime(x[0], '%d/%m/%Y')):
                        entries_text = f" ({data['entries']} entrées)" if data['entries'] > 1 else ""
                        response_text += f"\n- {date_str}: {data['total']:.2f} unités{entries_text}"
                
                if op_result is not None:
                    response_text += f"\n\n{op_explanation} = {op_result:.2f} unités."
            else:
                response_text = f"Aucune consommation de {famille} trouvée entre le {start_str} et le {end_str}."

    execution_time = round(time.time() - start_time, 2)
    print(f"TOTAL EXECUTION TIME: {execution_time} seconds")

    return {
        "computed": {
            "sum": round(aggregates['sum'], 2),
            "mean": round(aggregates['mean'], 2),
            "min": round(aggregates['min'], 2),
            "max": round(aggregates['max'], 2),
            "count": aggregates['count'],
            "date_type": date_type,
            "daily_breakdown": daily_breakdown if date_type == 'range' else None,
            "operation_requested": operation,
            "operation_result": round(op_result, 2) if op_result is not None and isinstance(op_result, (int, float)) else None,
            "operation_explanation": op_explanation
        },
        "rows": rows_preview,
        "response": response_text,
        "debug": debug_info,
        "execution_time": f"{execution_time} secondes",
        "performance": {
            "database_query_ms": query_time if USE_DATABASE else None,
            "total_ms": round(execution_time * 1000, 2)
        }
    }
