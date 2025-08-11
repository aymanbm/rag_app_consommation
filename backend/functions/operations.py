# def perform_operation(aggregates: dict, operation: dict):
#     op = operation.get('op')
#     v = operation.get('value')

#     if aggregates['count'] == 0:
#         return None, "Aucune donnée disponible pour cette période"
#     if op == 'none':
#         return None, None
    
#     if op == 'sum':
#         return aggregates['sum'], f"Somme = {aggregates['sum']:.2f}"
#     if op == 'average':
#         return aggregates['mean'], f"Moyenne = {aggregates['mean']:.2f}"
#     if op == 'min':
#         return aggregates['min'], f"Minimum = {aggregates['min']:.2f}"
#     if op == 'max':
#         return aggregates['max'], f"Maximum = {aggregates['max']:.2f}"
#     if op == 'count':
#         return aggregates['count'], f"Nombre d'entrées = {aggregates['count']}"
    
#     base = aggregates['sum']
#     if v is None:
#         return None, None
    
#     try:
#         if op == 'divide' and v != 0:
#             result = base / v
#             return result, f"{base:.2f} ÷ {v:.2f} = {result:.2f}"
#         elif op == 'multiply':
#             result = base * v
#             return result, f"{base:.2f} × {v:.2f} = {result:.2f}"
#         elif op == 'add':
#             result = base + v
#             return result, f"{base:.2f} + {v:.2f} = {result:.2f}"
#         elif op == 'subtract':
#             result = base - v
#             return result, f"{base:.2f} - {v:.2f} = {result:.2f}"
#     except Exception as e:
#         print(f"Operation error: {e}")
#         return None, None
    
#     return None, None