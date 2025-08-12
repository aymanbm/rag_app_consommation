from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import re
from typing import Tuple, Optional, Dict, Any
import calendar

def parse_smart_date_range(text: str, current_date: datetime = None) -> Tuple[Optional[datetime], Optional[datetime], str, Dict[str, Any]]:
    """
    Enhanced date parser that handles temporal expressions like 'last month', 'this week', etc.
    Returns: (start_date, end_date, date_type, temporal_info)
    """
    if current_date is None:
        current_date = datetime.now()
    
    text_lower = text.lower()
    temporal_info = {'is_temporal': False, 'temporal_type': None, 'comparison_periods': []}
    
    # First try existing date parsing for explicit dates
    try:
        from functions.parse_date import parse_date_range_from_text
        start_date, end_date, date_type = parse_date_range_from_text(text)
        if start_date and end_date:
            return start_date, end_date, date_type, temporal_info
    except:
        pass
    
    # Temporal patterns
    temporal_patterns = {
        # Current periods
        'today': ('today', 0),
        'aujourd\'hui': ('today', 0),
        'ce jour': ('today', 0),
        
        'this week': ('week', 0),
        'cette semaine': ('week', 0),
        'semaine actuelle': ('week', 0),
        
        'this month': ('month', 0),
        'ce mois': ('month', 0),
        'mois actuel': ('month', 0),
        'mois courant': ('month', 0),
        
        'this year': ('year', 0),
        'cette année': ('year', 0),
        'année actuelle': ('year', 0),
        
        # Past periods
        'yesterday': ('day', -1),
        'hier': ('day', -1),
        
        'last week': ('week', -1),
        'semaine derniere': ('week', -1),
        'semaine dernière': ('week', -1),
        'dernière semaine': ('week', -1),
        'derniere semaine': ('week', -1),
        'semaine passée': ('week', -1),
        
        'last month': ('month', -1),
        'mois dernier': ('month', -1),
        'dernier mois': ('month', -1),
        'mois passé': ('month', -1),
        
        'last year': ('year', -1),
        'année dernière': ('year', -1),
        'dernière année': ('year', -1),
        'année passée': ('year', -1),
        
        # Future periods
        'tomorrow': ('day', 1),
        'demain': ('day', 1),
        
        'next week': ('week', 1),
        'semaine prochaine': ('week', 1),
        
        'next month': ('month', 1),
        'mois prochain': ('month', 1),
        
        'next year': ('year', 1),
        'année prochaine': ('year', 1),
    }
    
    # Check for temporal patterns
    for pattern, (period_type, offset) in temporal_patterns.items():
        if pattern in text_lower:
            start_date, end_date = calculate_period_dates(current_date, period_type, offset)
            temporal_info['is_temporal'] = True
            temporal_info['temporal_type'] = f"{period_type}_{offset}"
            temporal_info['period_type'] = period_type
            temporal_info['offset'] = offset
            return start_date, end_date, 'range' if period_type != 'day' else 'single', temporal_info
    
    # Check for comparison patterns
    comparison_patterns = [
        r'(différence|compare|comparaison|comparer)\s+entre\s+(.+?)\s+et\s+(.+)',
        r'(ce mois|mois actuel|this month)\s+(vs|versus|contre|comparé à)\s+(mois dernier|last month)',
        r'(cette semaine|this week)\s+(vs|versus|contre|comparé à)\s+(semaine dernière|last week)',
    ]
    
    for pattern in comparison_patterns:
        match = re.search(pattern, text_lower)
        if match:
            temporal_info['is_temporal'] = True
            temporal_info['temporal_type'] = 'comparison'
            temporal_info['comparison_detected'] = True
            
            # Handle specific comparison cases
            if 'mois' in match.group(0):
                # This month vs last month
                current_month_start, current_month_end = calculate_period_dates(current_date, 'month', 0)
                last_month_start, last_month_end = calculate_period_dates(current_date, 'month', -1)
                
                temporal_info['comparison_periods'] = [
                    {'period': 'current_month', 'start': current_month_start, 'end': current_month_end},
                    {'period': 'last_month', 'start': last_month_start, 'end': last_month_end}
                ]
                return current_month_start, current_month_end, 'comparison', temporal_info
            
            elif 'semaine' in match.group(0):
                # This week vs last week
                current_week_start, current_week_end = calculate_period_dates(current_date, 'week', 0)
                last_week_start, last_week_end = calculate_period_dates(current_date, 'week', -1)
                
                temporal_info['comparison_periods'] = [
                    {'period': 'current_week', 'start': current_week_start, 'end': current_week_end},
                    {'period': 'last_week', 'start': last_week_start, 'end': last_week_end}
                ]
                return current_week_start, current_week_end, 'comparison', temporal_info
    
    # Check for relative date patterns (N days/weeks/months ago)
    relative_patterns = [
        r'il y a (\d+) jour[s]?',
        r'il y a (\d+) semaine[s]?',
        r'il y a (\d+) mois',
        r'(\d+) jour[s]? dernier[s]?',
        r'(\d+) semaine[s]? dernière[s]?',
        r'(\d+) mois dernier[s]?',
    ]
    
    for pattern in relative_patterns:
        match = re.search(pattern, text_lower)
        if match:
            number = int(match.group(1))
            if 'jour' in pattern:
                target_date = current_date - timedelta(days=number)
                temporal_info['is_temporal'] = True
                temporal_info['temporal_type'] = f'days_ago_{number}'
                return target_date, target_date, 'single', temporal_info
            elif 'semaine' in pattern:
                target_date = current_date - timedelta(weeks=number)
                start_date, end_date = calculate_period_dates(target_date, 'week', 0)
                temporal_info['is_temporal'] = True
                temporal_info['temporal_type'] = f'weeks_ago_{number}'
                return start_date, end_date, 'range', temporal_info
            elif 'mois' in pattern:
                target_date = current_date - relativedelta(months=number)
                start_date, end_date = calculate_period_dates(target_date, 'month', 0)
                temporal_info['is_temporal'] = True
                temporal_info['temporal_type'] = f'months_ago_{number}'
                return start_date, end_date, 'range', temporal_info
    
    return None, None, None, temporal_info

def calculate_period_dates(reference_date: datetime, period_type: str, offset: int) -> Tuple[datetime, datetime]:
    """Calculate start and end dates for a given period"""
    
    if period_type == 'day':
        target_date = reference_date + timedelta(days=offset)
        return target_date.replace(hour=0, minute=0, second=0, microsecond=0), \
               target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period_type == 'week':
        # Get Monday of the target week
        days_since_monday = reference_date.weekday()
        monday = reference_date - timedelta(days=days_since_monday)
        target_monday = monday + timedelta(weeks=offset)
        target_sunday = target_monday + timedelta(days=6)
        
        return target_monday.replace(hour=0, minute=0, second=0, microsecond=0), \
               target_sunday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    elif period_type == 'month':
        # First day of target month
        target_date = reference_date + relativedelta(months=offset)
        start_date = target_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # Last day of target month
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = start_date.replace(day=last_day, hour=23, minute=59, second=59, microsecond=999999)
        
        return start_date, end_date
    
    elif period_type == 'year':
        # First day of target year
        target_date = reference_date + relativedelta(years=offset)
        start_date = target_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end_date = target_date.replace(month=12, day=31, hour=23, minute=59, second=59, microsecond=999999)
        
        return start_date, end_date
    
    return reference_date, reference_date