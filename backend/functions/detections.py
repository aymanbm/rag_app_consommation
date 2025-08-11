from functions.normalize_text import normalize_text
from Database.database import initialize_data_source
from typing import Optional
import re, difflib

available_families, df_data = initialize_data_source()

def detect_famille_in_text(text: str) -> Optional[str]:
    text_norm = normalize_text(text)

    variations = {
        normalize_text("MAIS"): normalize_text("MAIS"),
        normalize_text("MAÏS"): normalize_text("MAIS"),
        normalize_text("CORN"): normalize_text("MAIS"),
        normalize_text("BLE FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLED FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLÉ FOURRAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("BLÉ FOURAGER"): normalize_text("BLE FOURRAGER"),
        normalize_text("ORG"): normalize_text("ORGE"),
        normalize_text("SOJA"): normalize_text("GRAINES DE SOJA"),
    }
    
    for variant, standard in variations.items():
        if variant in text_norm:
            return standard

    for fam in available_families:
        if fam == text_norm or fam in text_norm:
            return fam

    matches = difflib.get_close_matches(text_norm, available_families, n=1, cutoff=0.8)
    if matches:
        return matches[0]
    
    words = [w for w in re.split(r'[\s,;:.!?()]+', text_norm) if len(w) > 2]
    for w in words:
        matches = difflib.get_close_matches(w, available_families, n=1, cutoff=0.85)
        if matches:
            return matches[0]
    
    return None

def detect_math_operation(text: str):
    t = text.lower()
    
    number_patterns = [
        r'par\s+([-+]?\d+[.,]?\d*)',
        r'([-+]?\d+[.,]?\d*)\s*(?:fois|x|\*)',
        r'(?:diviser|divisé|divise)\s+par\s+([-+]?\d+[.,]?\d*)',
        r'(?:multiplier|multiplie|multiplié)\s+par\s+([-+]?\d+[.,]?\d*)',
        r'(?:ajouter|ajoute|ajouté)\s+([-+]?\d+[.,]?\d*)',
        r'(?:soustraire|soustrait|moins)\s+([-+]?\d+[.,]?\d*)'
    ]
    
    operand = None
    for pattern in number_patterns:
        match = re.search(pattern, t)
        if match:
            try:
                operand = float(match.group(1).replace(',', '.'))
                break
            except:
                continue
    
    operations = [
        (['divis', 'diviser', 'divisé', 'divise'], 'divide'),
        (['multipl', 'multiplié', 'multiplie', 'fois', 'x'], 'multiply'),
        (['ajout', 'ajoute', 'ajouter', 'plus', '+'], 'add'),
        (['soustrait', 'soustraire', 'moins', '-'], 'subtract'),
        (['somme', 'total', 'totaliser', 'additionner'], 'sum'),
        (['moyenn', 'moyenne', 'average'], 'average'),
        (['min', 'minim', 'minimum'], 'min'),
        (['max', 'maximum', 'maxim'], 'max'),
        (['nombre', 'count', 'compte', 'combien'], 'count')
    ]
    
    for keywords, op_type in operations:
        if any(keyword in t for keyword in keywords):
            return {'op': op_type, 'value': operand}
    
    return {'op': 'none', 'value': None}