from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import re

class IntelligentQueryAnalyzer:
    """Analyzes queries to determine if they need intelligent processing or simple responses"""
    
    def __init__(self, current_date: datetime = None):
        self.current_date = current_date or datetime.now()
        
    def analyze_query_complexity(self, query: str, temporal_info: Dict) -> Dict[str, Any]:
        """Determine what type of processing this query needs"""
        
        query_lower = query.lower()
        
        analysis = {
            'needs_intelligence': False,
            'complexity_type': 'simple',
            'reasoning_required': [],
            'comparison_needed': False,
            'summary_requested': False,
            'trend_analysis': False,
            'recommendations': False
        }
        
        # Intelligence triggers
        intelligence_patterns = {
            'comparison': [
                r'différence|compare|comparaison|comparer|vs|versus|contre',
                r'plus que|moins que|supérieur|inférieur',
                r'entre.*et',
                r'par rapport à'
            ],
            'analysis': [
                r'pourquoi|comment|expliquer|analyser',
                r'tendance|évolution|progression|régression',
                r'pattern|motif|comportement',
                r'insight|analyse|examen'
            ],
            'summary': [
                r'résumé|resume|synthèse|bilan|rapport',
                r'overview|aperçu|vue d\'ensemble',
                r'générer.*rapport|faire.*bilan',
                r'donner.*résumé'
            ],
            'predictions': [
                r'prédire|prévoir|estimer|projeter',
                r'prochaine|futur|avenir',
                r'tendance future|évolution future'
            ],
            'recommendations': [
                r'conseil|recommandation|suggestion',
                r'optimiser|améliorer|réduire',
                r'que faire|comment faire',
                r'stratégie|plan|action'
            ]
        }
        
        # Check for intelligence patterns
        for category, patterns in intelligence_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    analysis['needs_intelligence'] = True
                    analysis['reasoning_required'].append(category)
                    
                    if category == 'comparison':
                        analysis['comparison_needed'] = True
                        analysis['complexity_type'] = 'comparison'
                    elif category == 'summary':
                        analysis['summary_requested'] = True
                        analysis['complexity_type'] = 'summary'
                    elif category == 'analysis':
                        analysis['trend_analysis'] = True
                        analysis['complexity_type'] = 'analysis'
                    elif category == 'recommendations':
                        analysis['recommendations'] = True
                        analysis['complexity_type'] = 'recommendations'
        
        # Temporal comparisons automatically need intelligence
        if temporal_info.get('comparison_detected', False):
            analysis['needs_intelligence'] = True
            analysis['comparison_needed'] = True
            analysis['complexity_type'] = 'temporal_comparison'
            analysis['reasoning_required'].append('temporal_comparison')
        
        # Complex temporal queries need intelligence
        if temporal_info.get('is_temporal', False):
            temporal_type = temporal_info.get('temporal_type', '')
            if any(keyword in temporal_type for keyword in ['comparison', 'trend', 'evolution']):
                analysis['needs_intelligence'] = True
                analysis['reasoning_required'].append('temporal_intelligence')
        
        return analysis

def perform_intelligent_comparison(famille: str, period1_data: Dict, period2_data: Dict, 
                                 period1_info: Dict, period2_info: Dict) -> Dict[str, Any]:
    """Perform intelligent comparison between two periods"""
    
    comparison_result = {
        'period1': {
            'name': period1_info.get('period', 'Période 1'),
            'total': period1_data['sum'],
            'average': period1_data['mean'],
            'count': period1_data['count'],
            'min': period1_data['min'],
            'max': period1_data['max']
        },
        'period2': {
            'name': period2_info.get('period', 'Période 2'),
            'total': period2_data['sum'],
            'average': period2_data['mean'],
            'count': period2_data['count'],
            'min': period2_data['min'],
            'max': period2_data['max']
        },
        'differences': {},
        'analysis': [],
        'insights': []
    }
    
    # Calculate differences
    total_diff = period1_data['sum'] - period2_data['sum']
    total_pct = (total_diff / period2_data['sum'] * 100) if period2_data['sum'] != 0 else 0
    
    avg_diff = period1_data['mean'] - period2_data['mean']
    avg_pct = (avg_diff / period2_data['mean'] * 100) if period2_data['mean'] != 0 else 0
    
    comparison_result['differences'] = {
        'total_absolute': round(total_diff, 2),
        'total_percentage': round(total_pct, 2),
        'average_absolute': round(avg_diff, 2),
        'average_percentage': round(avg_pct, 2),
        'count_diff': period1_data['count'] - period2_data['count']
    }
    
    # Generate analysis
    analysis = []
    
    if abs(total_pct) > 10:
        direction = "augmenté" if total_diff > 0 else "diminué"
        analysis.append(f"Reception totale a {direction} de {abs(total_pct):.1f}% ({abs(total_diff):.2f} unités)")
    
    if abs(avg_pct) > 15:
        direction = "augmenté" if avg_diff > 0 else "diminué"
        analysis.append(f"Reception moyenne a {direction} de {abs(avg_pct):.1f}%")
    
    if comparison_result['differences']['count_diff'] != 0:
        count_diff = comparison_result['differences']['count_diff']
        direction = "plus" if count_diff > 0 else "moins"
        analysis.append(f"{abs(count_diff)} entrées en {direction}")
    
    # Generate insights
    insights = []
    
    if total_pct > 20:
        insights.append("⚠️ Augmentation significative de la Reception - vérifier les causes")
    elif total_pct < -20:
        insights.append("✅ Réduction importante de la Reception - efficacité améliorée")
    elif abs(total_pct) < 5:
        insights.append("📊 Reception stable entre les deux périodes")
    
    # Variability analysis
    period1_variability = (period1_data['max'] - period1_data['min']) / period1_data['mean'] if period1_data['mean'] > 0 else 0
    period2_variability = (period2_data['max'] - period2_data['min']) / period2_data['mean'] if period2_data['mean'] > 0 else 0
    
    if period1_variability > period2_variability * 1.5:
        insights.append("📈 Reception plus variable dans la période récente")
    elif period2_variability > period1_variability * 1.5:
        insights.append("📉 Reception plus stable dans la période récente")
    
    comparison_result['analysis'] = analysis
    comparison_result['insights'] = insights
    
    return comparison_result

def generate_intelligent_summary(famille: str, start_date: datetime, end_date: datetime, 
                               aggregates: Dict, daily_breakdown: Dict, 
                               current_date: datetime) -> Dict[str, Any]:
    """Generate intelligent summary with insights"""
    
    summary = {
        'period_info': {
            'famille': famille,
            'start_date': start_date.strftime('%d/%m/%Y'),
            'end_date': end_date.strftime('%d/%m/%Y'),
            'duration_days': (end_date - start_date).days + 1,
            'generated_on': current_date.strftime('%d/%m/%Y à %H:%M')
        },
        'key_metrics': {
            'total_reception': round(aggregates['sum'], 2),
            'daily_average': round(aggregates['mean'], 2),
            'peak_reception': round(aggregates['max'], 2),
            'lowest_reception': round(aggregates['min'], 2),
            'total_entries': aggregates['count']
        },
        'insights': [],
        'trends': [],
        'recommendations': []
    }
    
    # Generate insights
    insights = []
    
    # reception level analysis
    if aggregates['sum'] > 0:
        daily_avg = aggregates['sum'] / len(daily_breakdown) if daily_breakdown else aggregates['mean']
        
        if daily_avg > 200:
            insights.append("🔴 Reception élevée détectée")
        elif daily_avg > 100:
            insights.append("🟡 Reception modérée")
        else:
            insights.append("🟢 Reception faible")
    
    # Variability analysis
    if aggregates['count'] > 1:
        variability = (aggregates['max'] - aggregates['min']) / aggregates['mean'] if aggregates['mean'] > 0 else 0
        
        if variability > 2:
            insights.append("📊 Forte variabilité dans la Reception")
        elif variability < 0.5:
            insights.append("📊 Reception très stable")
        else:
            insights.append("📊 Variabilité normale")
    
    # Trend analysis from daily breakdown
    trends = []
    if daily_breakdown and len(daily_breakdown) >= 3:
        daily_values = [data['total'] for data in daily_breakdown.values()]
        
        # Simple trend detection
        first_half = daily_values[:len(daily_values)//2]
        second_half = daily_values[len(daily_values)//2:]
        
        avg_first = sum(first_half) / len(first_half)
        avg_second = sum(second_half) / len(second_half)
        
        trend_pct = ((avg_second - avg_first) / avg_first * 100) if avg_first > 0 else 0
        
        if trend_pct > 10:
            trends.append("📈 Tendance à la hausse sur la période")
        elif trend_pct < -10:
            trends.append("📉 Tendance à la baisse sur la période")
        else:
            trends.append("➡️ Tendance stable sur la période")
    
    # Recommendations
    recommendations = []
    
    if aggregates['max'] > aggregates['mean'] * 2:
        recommendations.append("💡 Investiguer les pics de Reception pour optimiser")
    
    if len(daily_breakdown) > 7:
        weekend_pattern = analyze_weekend_pattern(daily_breakdown, start_date)
        if weekend_pattern:
            recommendations.append(weekend_pattern)
    
    if aggregates['mean'] > 150:
        recommendations.append("⚡ Considérer des mesures d'économie d'énergie")
    
    summary['insights'] = insights
    summary['trends'] = trends
    summary['recommendations'] = recommendations
    
    return summary

def analyze_weekend_pattern(daily_breakdown: Dict, start_date: datetime) -> Optional[str]:
    """Analyze weekend vs weekday reception patterns"""
    
    weekday_totals = []
    weekend_totals = []
    
    for date_str, data in daily_breakdown.items():
        date_obj = datetime.strptime(date_str, '%d/%m/%Y')
        if date_obj.weekday() < 5:  # Monday = 0, Friday = 4
            weekday_totals.append(data['total'])
        else:  # Saturday, Sunday
            weekend_totals.append(data['total'])
    
    if weekday_totals and weekend_totals:
        avg_weekday = sum(weekday_totals) / len(weekday_totals)
        avg_weekend = sum(weekend_totals) / len(weekend_totals)
        
        diff_pct = ((avg_weekend - avg_weekday) / avg_weekday * 100) if avg_weekday > 0 else 0
        
        if diff_pct > 20:
            return "📅 Reception plus élevée le week-end (+{:.1f}%)".format(diff_pct)
        elif diff_pct < -20:
            return "📅 Reception plus faible le week-end ({:.1f}%)".format(diff_pct)
    
    return None