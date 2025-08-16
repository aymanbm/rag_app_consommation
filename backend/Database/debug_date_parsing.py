import sys
import os
import re
from datetime import datetime, date

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def debug_detection_workflow(user_input):
    """Debug the complete detection workflow"""
    print(f"\n{'='*80}")
    print(f"DEBUGGING COMPLETE WORKFLOW: '{user_input}'")
    print(f"{'='*80}")
    
    # Step 1: Parse the input
    print(f"\n1️⃣ INPUT PARSING:")
    print(f"   Raw input: '{user_input}'")
    
    # Extract potential identifiers and dates
    words = user_input.strip().split()
    print(f"   Split into words: {words}")
    
    # Try to identify the main identifier (product or silo)
    identifier = None
    date_str = None
    
    # Look for date patterns
    date_patterns = [
        r'(\d{1,2})/(\d{1,2})/(\d{4})',  # DD/MM/YYYY or MM/DD/YYYY
        r'(\d{1,2})/(\d{1,2})/(\d{2})',   # DD/MM/YY or MM/DD/YY
        r'(\d{4})-(\d{1,2})-(\d{1,2})',   # YYYY-MM-DD
        r'(\d{1,2})-(\d{1,2})-(\d{4})',   # DD-MM-YYYY
    ]
    
    for word in words:
        # Check if this word looks like a date
        is_date = False
        for pattern in date_patterns:
            if re.search(pattern, word):
                date_str = word
                is_date = True
                break
        
        # If not a date, might be the identifier
        if not is_date and not identifier:
            identifier = word
    
    print(f"   Extracted identifier: '{identifier}'")
    print(f"   Extracted date string: '{date_str}'")
    
    # Step 2: Parse the date
    print(f"\n2️⃣ DATE PARSING:")
    parsed_date = None
    
    if date_str:
        try:
            # Try DD/MM/YYYY format first (European style)
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    
                    # Handle 2-digit years
                    if year < 100:
                        year = 2000 + year if year < 50 else 1900 + year
                    
                    parsed_date = date(year, month, day)
                    print(f"   ✅ Parsed as DD/MM/YYYY: {parsed_date}")
                else:
                    print(f"   ❌ Invalid date format: {date_str}")
            
            elif '-' in date_str:
                # ISO format or DD-MM-YYYY
                parts = date_str.split('-')
                if len(parts) == 3:
                    if len(parts[0]) == 4:  # YYYY-MM-DD
                        year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    else:  # DD-MM-YYYY
                        day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    
                    parsed_date = date(year, month, day)
                    print(f"   ✅ Parsed date: {parsed_date}")
        
        except ValueError as e:
            print(f"   ❌ Date parsing error: {e}")
    else:
        print(f"   ⚠️ No date found in input")
    
    # Step 3: Determine if identifier is product or silo
    print(f"\n3️⃣ IDENTIFIER TYPE DETECTION:")
    
    if identifier:
        # This is where your detection logic should kick in
        # Let's simulate what your detect_libelle_prod_in_text and detect_silo_dest_in_text should do
        
        print(f"   Testing '{identifier}' against available products...")
        # Here you'd call your detection functions
        # For now, let's simulate the logic
        
        # Check if it's in available_libelle_prods (from your initialized data)
        try:
            from Database.database_receptions import available_libelle_prods, available_silo_destinations
            
            is_product = any(identifier.upper() in prod.upper() for prod in available_libelle_prods)
            is_silo = any(identifier.upper() in silo.upper() for silo in available_silo_destinations)
            
            print(f"   Product match: {'✅ YES' if is_product else '❌ NO'}")
            print(f"   Silo match: {'✅ YES' if is_silo else '❌ NO'}")
            
            if is_product and is_silo:
                print(f"   ⚠️ AMBIGUOUS: Found in both lists!")
            elif is_product:
                print(f"   🎯 IDENTIFIED AS: PRODUCT")
                query_type = "query_labelle"
            elif is_silo:
                print(f"   🎯 IDENTIFIED AS: SILO")
                query_type = "query_silo"
            else:
                print(f"   ❌ NOT FOUND: Neither product nor silo")
                query_type = None
                
        except ImportError as e:
            print(f"   ⚠️ Could not import detection data: {e}")
            query_type = None
    else:
        print(f"   ❌ No identifier found")
        query_type = None
    
    # Step 4: Show what the query should be
    print(f"\n4️⃣ EXPECTED QUERY:")
    
    if query_type and parsed_date and identifier:
        if query_type == "query_silo":
            print(f"   ✅ CORRECT QUERY:")
            print(f"      query_silo(")
            print(f"          start_date='{parsed_date}',")
            print(f"          end_date='{parsed_date}',")
            print(f"          silo_dest='{identifier}'")
            print(f"      )")
        elif query_type == "query_labelle":
            print(f"   ✅ CORRECT QUERY:")
            print(f"      query_labelle(")
            print(f"          start_date='{parsed_date}',")
            print(f"          end_date='{parsed_date}',")
            print(f"          libelle_prod='{identifier}'")
            print(f"      )")
    else:
        print(f"   ❌ CANNOT CONSTRUCT QUERY:")
        print(f"      - Query type: {query_type}")
        print(f"      - Parsed date: {parsed_date}")
        print(f"      - Identifier: {identifier}")
    
    # Step 5: Show what might be going wrong
    print(f"\n5️⃣ POTENTIAL ISSUES:")
    
    issues = []
    
    if not identifier:
        issues.append("❌ No identifier extracted from input")
    
    if not parsed_date:
        issues.append("❌ Date parsing failed")
    elif str(parsed_date) != "2022-08-27":  # Expected date from your example
        issues.append(f"⚠️ Date mismatch: got {parsed_date}, expected 2022-08-27")
    
    if not query_type:
        issues.append("❌ Could not determine if identifier is product or silo")
    
    if query_type == "query_labelle" and identifier == "1CD03":
        issues.append("⚠️ 1CD03 detected as product but should be silo")
    
    if not issues:
        issues.append("✅ No obvious issues detected")
    
    for issue in issues:
        print(f"   {issue}")
    
    return {
        'identifier': identifier,
        'date_str': date_str,
        'parsed_date': parsed_date,
        'query_type': query_type,
        'issues': issues
    }

def fix_query_reception_data():
    """Show the fix for query_reception_data function"""
    print(f"\n6️⃣ SUGGESTED FIX FOR query_reception_data():")
    
    print(f"""
   The issue is likely in your query_reception_data function.
   Current logic might be:
   
   def query_reception_data(start_date, end_date, labelle_prod, silo_dest, USE_DATABASE=USE_DATABASE):
       if labelle_prod != None:                    # ❌ Wrong condition
           return query_labelle(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)  # ❌ Wrong parameter
       elif silo_dest != None:
           return query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)
   
   FIXED VERSION:
   
   def query_reception_data(start_date, end_date, labelle_prod=None, silo_dest=None, USE_DATABASE=USE_DATABASE):
       if labelle_prod is not None:               # ✅ Fixed condition
           return query_labelle(start_date, end_date, labelle_prod, USE_DATABASE=USE_DATABASE)  # ✅ Fixed parameter
       elif silo_dest is not None:
           return query_silo(start_date, end_date, silo_dest, USE_DATABASE=USE_DATABASE)
       else:
           return query_general(start_date, end_date, USE_DATABASE=USE_DATABASE)
    """)

if __name__ == "__main__":
    # Test the specific case
    test_input = "1CD03 27/8/2022"
    result = debug_detection_workflow(test_input)
    fix_query_reception_data()
    
    print(f"\n{'='*80}")
    print(f"SUMMARY FOR '{test_input}':")
    print(f"{'='*80}")
    print(f"Expected: query_silo('2022-08-27', '2022-08-27', '1CD03')")
    print(f"Your error: 'Aucune reception de None trouvée pour le 22/08/2027'")
    print(f"Issues found: {len(result['issues'])} problems detected")