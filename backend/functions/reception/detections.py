from functions.normalize_text import normalize_text
from backend.Database.database_receptions import initialize_data_source
from typing import Optional
import re, difflib


available_libelle_prods, df_data = initialize_data_source()

def detect_libelle_prod_in_text(text: str) -> Optional[str]:
    text_norm = normalize_text(text)

    variations = {
        normalize_text("MAIS"): "MAIS",
        normalize_text("MAISE"): "MAIS",
        normalize_text("MAÏS"): "MAIS",

        normalize_text("MAIS AMERICAIN"): "MAIS AMERICAIN",
        normalize_text("MAIS AMERICA"): "MAIS AMERICAIN",
        normalize_text("MAIS AMERICAN"): "MAIS AMERICAIN",
        normalize_text("MAIS AMERICANE"): "MAIS AMERICAIN",
        normalize_text("MAIS AMERICAINE"): "MAIS AMERICAIN",
        normalize_text("MAISE AMERICAIN"): "MAIS AMERICAIN",
        normalize_text("MAÏS AMERICAIN"): "MAIS AMERICAIN",

        normalize_text("MAIS BRESILIEN"): "MAIS BRESILIEN",
        normalize_text("MAISE BRESILIEN"): "MAIS BRESILIEN",
        normalize_text("MAÏS BRESILIEN"): "MAIS BRESILIEN",

        normalize_text("MAIS ARGENTIN"): "MAIS ARGENTIN",
        normalize_text("MAIS ARGENTEN"): "MAIS ARGENTIN",
        normalize_text("MAIS ARGENTINE"): "MAIS ARGENTIN",
        normalize_text("MAISE ARGENTIN"): "MAIS ARGENTIN",
        normalize_text("MAÏS ARGENTIN"): "MAIS ARGENTIN",

        normalize_text("MAIS ROUMAIN"): "MAIS ROUMAIN",
        normalize_text("MAISE ROUMAIN"): "MAIS ROUMAIN",
        normalize_text("MAÏS ROUMAIN"): "MAIS ROUMAIN",

        normalize_text("Mais Broyé Fin"): "Mais Broyé Fin",
        normalize_text("MAIS BROYE"): "MAIS BROYE",
        normalize_text("MAIS BROY"): "MAIS BROYE",
        normalize_text("MAISE BROYE"): "MAIS BROYE",
        normalize_text("MAÏS BROYE"): "MAIS BROYE",

        normalize_text("MAIS UKRENIEN"): "MAIS UKRENIEN",
        normalize_text("MAIS UKREN"): "MAIS UKRENIEN",
        normalize_text("MAIS UKRENINE"): "MAIS UKRENIEN",
        normalize_text("MAISE UKRENIEN"): "MAIS UKRENIEN",
        normalize_text("MAÏS UKRENIEN"): "MAIS UKRENIEN",
        normalize_text("CORN"): "MAIS UKRENIEN",

        normalize_text("BLE FOURRAGER"): "BLE FOURRAGER",
        normalize_text("BLE FOURRAGER LOCAL"): "BLE FOURRAGER LOCAL",
        normalize_text("BLED FOURRAGER"): "BLE FOURRAGER",
        normalize_text("BLÉ FOURRAGER"): "BLE FOURRAGER",
        normalize_text("BLÉ FOURAGER"): "BLE FOURRAGER",
        normalize_text("BLÉ"): "BLE FOURRAGER",
        normalize_text("BLE"): "BLE FOURRAGER",


        normalize_text("ORG"): "ORGE",
        normalize_text("ORGE IMPORT"): "ORGE IMPORT",
        normalize_text("ORGE IMPORTE"): "ORGE IMPORT",
        normalize_text("ORGE LOCALE Q1"): "ORGE LOCALE Q1",
        normalize_text("ORGE LOCALE"): "ORGE LOCALE Q1",
        normalize_text("ORGE RUSSE"): "ORGE RUSSE",

        normalize_text("GRAINE DE SOJA EXTRUDEE"): "GRAINE DE SOJA EXTRUDEE",
        normalize_text("SOJA"): "GRAINES DE SOJA",
        normalize_text("GRAINES DE SOJA"): "GRAINES DE SOJA",
    }
    counter = 0
    length = 0
    the_one_variant = {}
    for variant, standard in variations.items():
        if variant in text_norm:
            counter += 1
            the_one_variant[len(variant)] = standard

        if counter > 1:
            list_length = list(the_one_variant.keys())
            max_length = max(list_length)
            print(f"DEBUG: Multiple variants detected in text '{text_norm}': {the_one_variant}")
            return the_one_variant[max_length]

    for libelle in available_libelle_prods:
        if libelle == text_norm or libelle in text_norm:
            print(f"DEBUG: Detected exact match for libelle '{libelle}'")
            return libelle

    matches = difflib.get_close_matches(text_norm, available_libelle_prods, n=1, cutoff=0.8)
    if matches:
        return matches[0]
    
    words = [w for w in re.split(r'[\s,;:.!?()]+', text_norm) if len(w) > 2]
    print(f"DEBUG: Words extracted from text: {words}")
    for w in words:
        matches = difflib.get_close_matches(w, available_libelle_prods, n=1, cutoff=0.85)
        if matches:
            return matches[0]
    
    return None

available_silo_destinations, df_data = initialize_data_source(thing="SILO_DEST")

def detect_silo_dest_in_text(text: str) -> Optional[str]:
    text_norm = normalize_text(text)

    variations = ['2CD06', '1SN12', '1SN08', '1SC09', '1S08', '1SN14', '1SC07',
       '1S07', '1SN10', '1SN09', '1S06', '2S01', '1S02', '1CD03', '2S02',
       '1S09', '1SN04', '1SC04', '2CD28', '2CD03', '2CD01', '1SN05',
       '1SN01', '1SN15', '2CD07', '1SN02', '1SN16', '1S05', '1SC01',
       '1S03', '2S08', '1SN03', '1SC03', '1SN07', '1SN13', '2S10', '2S09',
       '2S07', '1SN06', '2CD26', '1CD06', '1SC08', '1SP04', '1SC05',
       '1SC06', '1SC02', '1CD02', '1SN11', '1SC10', '2CD23', '2CD21',
       '2CD08', '2CD17', 'SAC6', '1CD04', '2CD02', 'SAC14', '1CD15',
       '2CD15', '1CD07', '2SP09', '2CD12', '1CD12', '1CD14', '2CD09',
       '2CD27', '2SP08', '2SP02', '1CD05', '1CD09', '2CD24', 'SAC1',
       '1CD08', '2CD10', '2CD16', '2CD20', 'SAC3', '2CD29', '1BCH2',
       'SAC8', '1CD13', '1SP03', '2SP07', '1CD11', '1CD01', '1SP08',
       '1BCH1', 'SAC4', '2CD11']

    for standard in variations:
        if standard in text_norm:
            return standard

    for fam in available_silo_destinations:
        if fam == text_norm or fam in text_norm:
            return fam

    matches = difflib.get_close_matches(text_norm, available_silo_destinations, n=1, cutoff=0.8)
    if matches:
        return matches[0]
    
    words = [w for w in re.split(r'[\s,;:.!?()]+', text_norm) if len(w) > 2]
    for w in words:
        matches = difflib.get_close_matches(w, available_silo_destinations, n=1, cutoff=0.85)
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