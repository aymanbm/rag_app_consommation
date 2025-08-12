# query_execute.py
from functions.normalize_text import normalize_text
from backend.functions.consommation.detections import detect_famille_in_text
import time
from pydantic import BaseModel
from backend.Database.database_consommation import query_consumption_data
import pandas as pd
from datetime import datetime
from typing import Optional

# Import our new intelligent modules
from backend.functions.smart_date_parser import parse_smart_date_range
from backend.functions.enhanced_operations import detect_multiple_operations, perform_multiple_operations
from backend.functions.consommation.intelligent_analyzer import (
    IntelligentQueryAnalyzer, 
    perform_intelligent_comparison,
    generate_intelligent_summary
)

class Question(BaseModel):
    question: str
    mode: Optional[str] = None

async def query_exact_intelligent(q: Question, USE_DATABASE=True, AGGREGATION_STRATEGY="hybrid"):
    start_time = time.time()
    current_date = datetime.now()  # 🔥 CURRENT DATE ACCESS
    q_text: str = q.question or ""
    debug_info: dict = {}

    print(f"\nQUERY START: {q_text}")
    print(f"CURRENT DATE: {current_date.strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Initialize intelligent analyzer
    analyzer = IntelligentQueryAnalyzer(current_date)
    
    # Smart date parsing with current date awareness
    start_date, end_date, date_type, temporal_info = parse_smart_date_range(q_text, current_date)
    
    # Detect family
    famille = detect_famille_in_text(q_text)
    
    debug_info.update({
        'normalized_question': normalize_text(q_text),
        'current_date': current_date.strftime('%d/%m/%Y %H:%M:%S'),
        'parsed_start': str(start_date) if start_date else None,
        'parsed_end': str(end_date) if end_date else None,
        'date_type': date_type,
        'detected_family': famille,
        'temporal_info': temporal_info
    })

    # Early validation
    if not start_date or not end_date:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": f"Erreur: Date non trouvée. Date actuelle: {current_date.strftime('%d/%m/%Y')}. Essayez 'Dernier année', 'cette semaine', ou '03/06/2024'",
            "debug": debug_info,
            "execution_time": f"{execution_time} secondes"
        }

    if not famille:
        execution_time = round(time.time() - start_time, 2)
        return {
            "response": "Famille non trouvée. Formats acceptés: MAIS, ORGE, BLE FOURRAGER, GRAINES DE SOJA.",
            "debug": debug_info,
            "execution_time": f"{execution_time} secondes"
        }

    # Analyze query complexity
    complexity_analysis = analyzer.analyze_query_complexity(q_text, temporal_info)
    print(f"COMPLEXITY ANALYSIS: {complexity_analysis}")

    # Handle comparison queries (special case)
    if date_type == 'comparison' and temporal_info.get('comparison_periods'):
        return await handle_comparison_query(
            q_text, famille, temporal_info, complexity_analysis, 
            USE_DATABASE, current_date, start_time, debug_info
        )

    # Regular data query
    query_start = time.time()
    data_result = query_consumption_data(start_date, end_date, famille, USE_DATABASE)
    query_time = round((time.time() - query_start) * 1000, 2)
    print(f"Database query took: {query_time}ms")

    # Process data
    aggregates, rows_preview, daily_breakdown = process_data_result(
        data_result, USE_DATABASE, date_type
    )

    # Detect operations
    operations_info = detect_multiple_operations(q_text)
    operations_result = perform_multiple_operations(aggregates, operations_info)

    # Generate response based on complexity
    if complexity_analysis['needs_intelligence']:
        response_text = await generate_intelligent_response(
            q_text, famille, start_date, end_date, date_type,
            aggregates, daily_breakdown, operations_result,
            complexity_analysis, current_date
        )
    else:
        # Fast simple response
        response_text = generate_simple_response(
            famille, start_date, end_date, date_type,
            aggregates, daily_breakdown, operations_result
        )

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
            "operations_detected": operations_info['operations'],
            "operation_results": operations_result['results'],
            "intelligence_used": complexity_analysis['needs_intelligence'],
            "complexity_type": complexity_analysis['complexity_type']
        },
        "rows": rows_preview,
        "response": response_text,
        "debug": debug_info,
        "execution_time": f"{execution_time} secondes",
        "performance": {
            "database_query_ms": query_time if USE_DATABASE else None,
            "total_ms": round(execution_time * 1000, 2),
            "intelligence_used": complexity_analysis['needs_intelligence']
        }
    }

async def handle_comparison_query(q_text, famille, temporal_info, complexity_analysis, 
                                USE_DATABASE, current_date, start_time, debug_info):
    """Handle comparison queries between two periods"""
    
    comparison_periods = temporal_info['comparison_periods']
    period1 = comparison_periods[0]
    period2 = comparison_periods[1]
    
    # Query data for both periods
    data1 = query_consumption_data(period1['start'], period1['end'], famille, USE_DATABASE)
    data2 = query_consumption_data(period2['start'], period2['end'], famille, USE_DATABASE)
    
    # Process both datasets
    aggregates1, _, _ = process_data_result(data1, USE_DATABASE, 'range')
    aggregates2, _, _ = process_data_result(data2, USE_DATABASE, 'range')
    
    # Perform intelligent comparison
    comparison_result = perform_intelligent_comparison(
        famille, aggregates1, aggregates2, period1, period2
    )
    
    # Generate comparison response
    response_text = generate_comparison_response(famille, comparison_result, current_date)
    
    execution_time = round(time.time() - start_time, 2)
    
    return {
        "computed": {
            "comparison_data": comparison_result,
            "period1_data": aggregates1,
            "period2_data": aggregates2,
            "intelligence_used": True,
            "complexity_type": "comparison"
        },
        "response": response_text,
        "debug": debug_info,
        "execution_time": f"{execution_time} secondes"
    }

def process_data_result(data_result, USE_DATABASE, date_type):
    """Process data result into standardized format"""
    
    if USE_DATABASE:
        aggregates = data_result['aggregates']
        rows_preview = data_result['sample_rows']
        daily_data = data_result['daily_breakdown']
        
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
    
    return aggregates, rows_preview, daily_breakdown

async def generate_intelligent_response(q_text, famille, start_date, end_date, date_type,
                                      aggregates, daily_breakdown, operations_result,
                                      complexity_analysis, current_date):
    """Generate intelligent response using analysis"""
    
    if complexity_analysis['summary_requested']:
        # Generate comprehensive summary
        summary = generate_intelligent_summary(
            famille, start_date, end_date, aggregates, daily_breakdown, current_date
        )
        
        return format_summary_response(summary)
    
    elif complexity_analysis['trend_analysis']:
        # Generate trend analysis
        return generate_trend_analysis_response(
            famille, start_date, end_date, aggregates, daily_breakdown
        )
    
    else:
        # Default intelligent response
        return generate_simple_response(
            famille, start_date, end_date, date_type,
            aggregates, daily_breakdown, operations_result
        )

def generate_simple_response(famille, start_date, end_date, date_type, 
                           aggregates, daily_breakdown, operations_result):
    """Generate simple response for basic queries"""
    
    if aggregates['count'] == 0:
        if date_type == 'single':
            date_str = start_date.strftime("%d/%m/%Y")
            return f"Aucune consommation de {famille} trouvée pour le {date_str}."
        else:
            start_str = start_date.strftime("%d/%m/%Y")
            end_str = end_date.strftime("%d/%m/%Y")
            return f"Aucune consommation de {famille} trouvée entre le {start_str} et le {end_str}."
    
    if date_type == 'single':
        date_str = start_date.strftime("%d/%m/%Y")
        response = f"Consommation de {famille} le {date_str}: "
    else:
        start_str = start_date.strftime("%d/%m/%Y")
        end_str = end_date.strftime("%d/%m/%Y")
        response = f"Consommation de {famille} du {start_str} au {end_str}: "
    
    if operations_result['is_multiple']:
        response += "\n" + "\n".join([f"• {exp}" for exp in operations_result['explanations']])
        response += f"\n\n({aggregates['count']} entrées au total)"
    else:
        primary_explanation = operations_result['explanations'][0] if operations_result['explanations'] else f"Total = {aggregates['sum']:.2f} tonnes"
        response += primary_explanation
        if aggregates['count'] > 1:
            response += f" (sur {aggregates['count']} entrées)"
    
    return response

def generate_comparison_response(famille, comparison_result, current_date):
    """Generate comparison response"""
    
    p1 = comparison_result['period1']
    p2 = comparison_result['period2']
    diff = comparison_result['differences']
    
    response = f"📊 COMPARAISON CONSOMMATION {famille.upper()}\n"
    response += f"Générée le {current_date.strftime('%d/%m/%Y à %H:%M')}\n\n"
    
    response += f"📈 {p1['name'].replace('_', ' ').title()}: {p1['total']:.2f} tonnes\n"
    response += f"📊 {p2['name'].replace('_', ' ').title()}: {p2['total']:.2f} tonnes\n\n"
    
    direction = "📈 AUGMENTATION" if diff['total_absolute'] > 0 else "📉 DIMINUTION"
    response += f"{direction}: {abs(diff['total_absolute']):.2f} tonnes ({abs(diff['total_percentage']):.1f}%)\n\n"
    
    if comparison_result['analysis']:
        response += "🔍 ANALYSE:\n"
        for analysis in comparison_result['analysis']:
            response += f"• {analysis}\n"
        response += "\n"
    
    if comparison_result['insights']:
        response += "💡 INSIGHTS:\n"
        for insight in comparison_result['insights']:
            response += f"• {insight}\n"
    
    return response

def format_summary_response(summary):
    """Format summary into readable response"""
    
    period = summary['period_info']
    metrics = summary['key_metrics']
    
    response = f"📋 RÉSUMÉ CONSOMMATION {period['famille'].upper()}\n"
    response += f"Période: {period['start_date']} au {period['end_date']} ({period['duration_days']} jours)\n"
    response += f"Généré le {period['generated_on']}\n\n"
    
    response += "📊 MÉTRIQUES CLÉS:\n"
    response += f"• Total: {metrics['total_consumption']:.2f} tonnes\n"
    response += f"• Moyenne quotidienne: {metrics['daily_average']:.2f} tonnes\n"
    response += f"• Pic: {metrics['peak_consumption']:.2f} tonnes\n"
    response += f"• Minimum: {metrics['lowest_consumption']:.2f} tonnes\n"
    response += f"• Nombre d'entrées: {metrics['total_entries']}\n\n"
    
    if summary['insights']:
        response += "🔍 INSIGHTS:\n"
        for insight in summary['insights']:
            response += f"• {insight}\n"
        response += "\n"
    
    if summary['trends']:
        response += "📈 TENDANCES:\n"
        for trend in summary['trends']:
            response += f"• {trend}\n"
        response += "\n"
    
    if summary['recommendations']:
        response += "💡 RECOMMANDATIONS:\n"
        for rec in summary['recommendations']:
            response += f"• {rec}\n"
    
    return response

def generate_trend_analysis_response(famille, start_date, end_date, aggregates, daily_breakdown):
    """Generate trend analysis response"""
    
    response = f"📈 ANALYSE TENDANCE {famille.upper()}\n"
    response += f"Période: {start_date.strftime('%d/%m/%Y')} au {end_date.strftime('%d/%m/%Y')}\n\n"
    
    if daily_breakdown and len(daily_breakdown) >= 3:
        daily_values = [data['total'] for data in daily_breakdown.values()]
        
        # Calculate trend
        first_third = daily_values[:len(daily_values)//3]
        last_third = daily_values[-len(daily_values)//3:]
        
        avg_start = sum(first_third) / len(first_third)
        avg_end = sum(last_third) / len(last_third)
        
        trend_pct = ((avg_end - avg_start) / avg_start * 100) if avg_start > 0 else 0
        
        response += "📊 MÉTRIQUES:\n"
        response += f"• Moyenne début période: {avg_start:.2f} tonnes/jour\n"
        response += f"• Moyenne fin période: {avg_end:.2f} tonnes/jour\n"
        response += f"• Variation: {trend_pct:+.1f}%\n\n"
        
        if trend_pct > 15:
            response += "🔴 TENDANCE FORTE À LA HAUSSE\n"
            response += "• Consommation en augmentation significative\n"
            response += "• Investigation recommandée pour identifier les causes\n"
        elif trend_pct > 5:
            response += "🟡 TENDANCE LÉGÈRE À LA HAUSSE\n"
            response += "• Augmentation modérée de la consommation\n"
        elif trend_pct < -15:
            response += "🟢 TENDANCE FORTE À LA BAISSE\n"
            response += "• Réduction significative de la consommation\n"
            response += "• Efficacité énergétique en amélioration\n"
        elif trend_pct < -5:
            response += "🟡 TENDANCE LÉGÈRE À LA BAISSE\n"
            response += "• Diminution modérée de la consommation\n"
        else:
            response += "➡️ TENDANCE STABLE\n"
            response += "• Consommation constante sur la période\n"
        
        # Volatility analysis
        volatility = (max(daily_values) - min(daily_values)) / (sum(daily_values) / len(daily_values))
        response += f"\n📊 VOLATILITÉ: {volatility:.2f}\n"
        
        if volatility > 1.5:
            response += "• Forte variabilité - consommation irrégulière\n"
        elif volatility > 0.8:
            response += "• Variabilité modérée\n"
        else:
            response += "• Faible variabilité - consommation régulière\n"
    
    else:
        response += "Période trop courte pour une analyse de tendance détaillée.\n"
        response += f"Total consommé: {aggregates['sum']:.2f} tonnes\n"
        response += f"Moyenne: {aggregates['mean']:.2f} tonnes\n"
    
    return response