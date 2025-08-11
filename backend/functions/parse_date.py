import re
from dateutil import parser as dateutil_parser

def parse_date_range_from_text(text: str):
    text = text.strip()
    
    range_patterns = [
        r'du\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à)\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'entre\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+et\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'de\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à|jusqu\'au|jusqu\s+au)\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
        r'(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\s+(?:au|à|-|jusqu\'au|jusqu\s+au)\s+(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})'
    ]
    
    for pattern in range_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                d1 = dateutil_parser.parse(m.group(1), dayfirst=True).date()
                d2 = dateutil_parser.parse(m.group(2), dayfirst=True).date()
                return (min(d1, d2), max(d1, d2), 'range')
            except:
                continue
    
    single_date_patterns = [
        r'(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',
        r'(?:au|à)\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})(?!\s*(?:au|à|-|jusqu))',
        r'pour\s+(?:le\s+)?(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})',
    ]
    
    for pattern in single_date_patterns:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                d = dateutil_parser.parse(m.group(1), dayfirst=True).date()
                return (d, d, 'single')
            except:
                continue
    
    dates = re.findall(r'\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}', text)
    parsed = []
    for ds in dates:
        try:
            parsed.append(dateutil_parser.parse(ds, dayfirst=True).date())
        except:
            continue
    
    if len(parsed) >= 2:
        return (min(parsed[0], parsed[1]), max(parsed[0], parsed[1]), 'range')
    elif len(parsed) == 1:
        return (parsed[0], parsed[0], 'single')
    
    return (None, None, None)