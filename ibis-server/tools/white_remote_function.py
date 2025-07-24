import requests
import json
import time
from typing import List, Dict, Tuple

def load_test_queries(json_file_path: str) -> List[Dict]:
    try:
        with open(json_file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Error: File '{json_file_path}' not found")
        return []
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file '{json_file_path}'")
        return []

def test_sql_query(sql: str, api_url: str, connection_info: Dict, manifest_str: str) -> Tuple[int, str, str]:
    payload = {
        "connectionInfo": connection_info,
        "manifestStr": manifest_str,
        "sql": sql
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        error_type = categorize_error(response.status_code, response.text)
        return response.status_code, response.text, error_type
    except requests.exceptions.RequestException as e:
        return -1, str(e), "network_error"

def categorize_error(status_code: int, response_text: str) -> str:
    """
    Categorize the type of error based on status code and response content
    """
    if status_code == 200:
        return "success"
    
    if status_code != 500:
        return "other_http_error"
    
    # Parse response to check error type for 500 errors
    response_lower = response_text.lower()
    
    # Common patterns for "function not found" errors
    function_not_found_patterns = [
        "function not found",
        "function does not exist",
        "unknown function",
        "unrecognized function",
        "no such function",
        "function is not defined",
        "undefined function",
        "invalid function name",
        "function not available",
        "function not supported"
    ]
    
    for pattern in function_not_found_patterns:
        if pattern in response_lower:
            return "function_not_found"
    
    # Common patterns for syntax errors (my SQL mistakes)
    syntax_error_patterns = [
        "syntax error",
        "parse error",
        "invalid syntax",
        "unexpected token",
        "malformed query",
        "invalid sql",
        "parsing failed",
        "sql compilation error",
        "invalid expression",
        "expected",
        "unexpected"
    ]
    
    for pattern in syntax_error_patterns:
        if pattern in response_lower:
            return "syntax_error"
    
    # Other 500 errors
    return "other_500_error"

def main():
    # Configuration
    API_URL = "http://localhost:8000/v3/connector/bigquery/query"
    JSON_FILE_PATH = "" # Path to your JSON file with test queries


    CONNECTION_INFO = {
            
    } # Add your connection info here, e.g., credentials, project ID, etc.
    
    MANIFEST_STR = "" # Add your manifest string here if needed, otherwise leave empty
        
    # Load test queries from JSON file
    print(f"Loading test queries from: {JSON_FILE_PATH}")
    test_queries = load_test_queries(JSON_FILE_PATH)
    
    if not test_queries:
        print("No test queries loaded. Exiting.")
        return
    
    print(f"Loaded {len(test_queries)} test queries")
    print("Starting API tests...\n")
    
    # Results tracking
    results = {
        "success": [],
        "function_not_found": [],
        "syntax_error": [],
        "other_500_error": [],
        "other_http_error": [],
        "network_errors": []
    }
    
    # Test each query
    for i, query in enumerate(test_queries, 1):
        function_name = query.get("function_name", f"query_{query.get('id', i)}")
        sql = query.get("sql", "")
        
        if not sql:
            print(f"[{i}/{len(test_queries)}] Skipping {function_name} - No SQL found")
            continue
        
        print(f"[{i}/{len(test_queries)}] Testing {function_name}...", end=" ")
        
        # Test the query
        status_code, response_text, error_type = test_sql_query(sql, API_URL, CONNECTION_INFO, MANIFEST_STR)
        
        # Categorize results
        query_result = {
            "id": query.get("id"),
            "function_name": function_name,
            "sql": sql,
            "status_code": status_code,
            "response": response_text
        }
        
        if error_type == "success":
            results["success"].append(query_result)
            print("✅ Success")
        elif error_type == "function_not_found":
            results["function_not_found"].append(query_result)
            print("❌ Function Not Found")
        elif error_type == "syntax_error":
            results["syntax_error"].append(query_result)
            print("🔧 Syntax Error (SQL issue)")
        elif error_type == "other_500_error":
            results["other_500_error"].append(query_result)
            print("❌ Other 500 Error")
        elif error_type == "other_http_error":
            results["other_http_error"].append(query_result)
            print(f"❌ HTTP {status_code}")
        elif error_type == "network_error":
            results["network_errors"].append(query_result)
            print("❌ Network Error")
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.1)
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Total queries tested: {len(test_queries)}")
    print(f"✅ Successful: {len(results['success'])}")
    print(f"❌ Function Not Found: {len(results['function_not_found'])}")
    print(f"🔧 Syntax Errors (SQL issues): {len(results['syntax_error'])}")
    print(f"❌ Other 500 Errors: {len(results['other_500_error'])}")
    print(f"❌ Other HTTP Errors: {len(results['other_http_error'])}")
    print(f"❌ Network Errors: {len(results['network_errors'])}")
    
    # Print function not found errors (what you're looking for)
    if results["function_not_found"]:
        print("\n" + "="*60)
        print("FUNCTIONS NOT FOUND (Unsupported Functions)")
        print("="*60)
        for error in results["function_not_found"]:
            print(f"ID: {error['id']} | Function: {error['function_name']}")
            print(f"SQL: {error['sql']}")
            print(f"Error: {error['response'][:150]}..." if len(error['response']) > 150 else f"Error: {error['response']}")
            print("-" * 40)
    
    # Print syntax errors (my SQL mistakes that need fixing)
    if results["syntax_error"]:
        print("\n" + "="*60)
        print("SYNTAX ERRORS (SQL Issues - Need to Fix)")
        print("="*60)
        for error in results["syntax_error"]:
            print(f"ID: {error['id']} | Function: {error['function_name']}")
            print(f"SQL: {error['sql']}")
            print(f"Error: {error['response'][:150]}..." if len(error['response']) > 150 else f"Error: {error['response']}")
            print("-" * 40)
    
    # Print other 500 errors for investigation
    if results["other_500_error"]:
        print("\n" + "="*60)
        print("OTHER 500 ERRORS (Need Investigation)")
        print("="*60)
        for error in results["other_500_error"]:
            print(f"ID: {error['id']} | Function: {error['function_name']}")
            print(f"SQL: {error['sql']}")
            print(f"Error: {error['response'][:150]}..." if len(error['response']) > 150 else f"Error: {error['response']}")
            print("-" * 40)
    
    # Save detailed results to file
    output_file = "api_test_results2.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDetailed results saved to: {output_file}")
    
    # Print summary lists for quick reference
    if results["function_not_found"]:
        print("\n🎯 UNSUPPORTED FUNCTIONS (what you're looking for):")
        unsupported_functions = [error['function_name'] for error in results['function_not_found']]
        print(", ".join(unsupported_functions))
    
    if results["syntax_error"]:
        print("\n🔧 FUNCTIONS WITH SQL SYNTAX ISSUES (need to fix the test SQL):")
        syntax_error_functions = [error['function_name'] for error in results['syntax_error']]
        print(", ".join(syntax_error_functions))
    
    if results["success"]:
        print(f"\n✅ SUPPORTED FUNCTIONS ({len(results['success'])} total):")
        supported_functions = [result['function_name'] for result in results['success']]
        print(", ".join(supported_functions))

if __name__ == "__main__":
    main()