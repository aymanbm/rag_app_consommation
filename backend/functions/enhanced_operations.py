import re
from typing import List, Dict, Any
from datetime import datetime

def detect_multiple_operations(text: str) -> Dict[str, Any]:
    """Detect multiple operations in a single query"""
    t = text.lower()
    
    # Initialize result
    operations = []
    primary_operation = None
    
    # Define operation patterns and their keywords
    operation_patterns = {
        'max': ['max', 'maximum', 'maximal', 'plus grand', 'plus élevé', 'le plus'],
        'min': ['min', 'minimum', 'minimal', 'plus petit', 'plus bas', 'le moins'],
        'mean': ['moyenne', 'moyenn', 'average', 'moy'],
        'sum': ['somme', 'total', 'totaliser', 'additionner', 'addition'],
        'count': ['nombre', 'count', 'compte', 'combien', 'quantité']
    }
    
    # Check for each operation type
    for op_type, keywords in operation_patterns.items():
        for keyword in keywords:
            if keyword in t:
                operations.append(op_type)
                if primary_operation is None:  # First found is primary
                    primary_operation = op_type
                break
    
    # Remove duplicates while preserving order
    operations = list(dict.fromkeys(operations))
    
    # Check for explicit "avec" (with) patterns that indicate multiple operations
    avec_patterns = [
        r'avec\s+(min|minimum|max|maximum|moyenne|total|somme|count|nombre)',
        r'et\s+(min|minimum|max|maximum|moyenne|total|somme|count|nombre)',
        r'plus\s+(min|minimum|max|maximum|moyenne|total|somme|count|nombre)',
        r'ainsi que\s+(min|minimum|max|maximum|moyenne|total|somme|count|nombre)'
    ]
    
    for pattern in avec_patterns:
        matches = re.findall(pattern, t)
        for match in matches:
            additional_op = normalize_operation_name(match)
            if additional_op and additional_op not in operations:
                operations.append(additional_op)
    
    # Handle special combinations
    if 'min et max' in t or 'max et min' in t:
        if 'min' not in operations:
            operations.append('min')
        if 'max' not in operations:
            operations.append('max')
    
    return {
        'operations': operations,
        'primary_operation': primary_operation or (operations[0] if operations else 'sum'),
        'is_multiple': len(operations) > 1
    }

def normalize_operation_name(op_text: str) -> str:
    """Convert operation text to standard operation name"""
    op_text = op_text.lower().strip()
    
    if op_text in ['max', 'maximum', 'maximal']:
        return 'max'
    elif op_text in ['min', 'minimum', 'minimal']:
        return 'min'
    elif op_text in ['moyenne', 'moyenn', 'average', 'moy']:
        return 'mean'
    elif op_text in ['somme', 'total', 'totaliser']:
        return 'sum'
    elif op_text in ['nombre', 'count', 'compte', 'combien']:
        return 'count'
    
    return None

def perform_multiple_operations(aggregates: dict, operations_info: dict) -> Dict[str, Any]:
    """Perform multiple operations and return results"""
    
    if aggregates['count'] == 0:
        return {
            'results': {},
            'explanations': [],
            'primary_result': None,
            'formatted_response': "Aucune donnée disponible pour cette période"
        }
    
    operations = operations_info['operations']
    results = {}
    explanations = []
    
    # Perform each operation
    for op in operations:
        if op == 'sum':
            results[op] = aggregates['sum']
            explanations.append(f"Somme = {aggregates['sum']:.2f} unités")
        elif op == 'mean':
            results[op] = aggregates['mean']
            explanations.append(f"Moyenne = {aggregates['mean']:.2f} unités")
        elif op == 'min':
            results[op] = aggregates['min']
            explanations.append(f"Minimum = {aggregates['min']:.2f} unités")
        elif op == 'max':
            results[op] = aggregates['max']
            explanations.append(f"Maximum = {aggregates['max']:.2f} unités")
        elif op == 'count':
            results[op] = aggregates['count']
            explanations.append(f"Nombre d'entrées = {aggregates['count']}")
    
    # Primary result is the first operation requested
    primary_op = operations_info['primary_operation']
    primary_result = results.get(primary_op)
    
    return {
        'results': results,
        'explanations': explanations,
        'primary_result': primary_result,
        'primary_operation': primary_op,
        'is_multiple': operations_info['is_multiple']
    }

def generate_response_without_llm(famille: str, start_date, end_date, date_type: str, 
                                aggregates: dict, daily_breakdown: dict, 
                                operations_result: dict) -> str:
    """Generate comprehensive response without using LLM"""
    
    if aggregates['count'] == 0:
        if date_type == 'single':
            date_str = start_date.strftime("%d/%m/%Y")
            return f"Aucune consommation de {famille} trouvée pour le {date_str}."
        else:
            start_str = start_date.strftime("%d/%m/%Y")
            end_str = end_date.strftime("%d/%m/%Y")
            return f"Aucune consommation de {famille} trouvée entre le {start_str} et le {end_str}."
    
    # Build response based on date type
    if date_type == 'single':
        date_str = start_date.strftime("%d/%m/%Y")
        response = f"Consommation de {famille} le {date_str}:\n\n"
    else:
        start_str = start_date.strftime("%d/%m/%Y")
        end_str = end_date.strftime("%d/%m/%Y")
        response = f"Consommation de {famille} du {start_str} au {end_str}:\n\n"
    
    # Add operation results
    if operations_result['is_multiple']:
        response += "Résultats demandés:\n"
        for explanation in operations_result['explanations']:
            response += f"• {explanation}\n"
        response += f"\n({aggregates['count']} entrées au total)"
    else:
        # Single operation
        primary_explanation = operations_result['explanations'][0] if operations_result['explanations'] else f"Total = {aggregates['sum']:.2f} unités"
        response += primary_explanation
        if aggregates['count'] > 1:
            response += f" (sur {aggregates['count']} entrées)"
    
    # Add daily breakdown for ranges if reasonable size
    if date_type == 'range' and daily_breakdown and len(daily_breakdown) <= 10:
        response += "\n\nDétail par jour:"
        for date_str, data in sorted(daily_breakdown.items(), 
                                   key=lambda x: datetime.strptime(x[0], '%d/%m/%Y')):
            entries_text = f" ({data['entries']} entrées)" if data['entries'] > 1 else ""
            response += f"\n• {date_str}: {data['total']:.2f} unités{entries_text}"
    
    return response





