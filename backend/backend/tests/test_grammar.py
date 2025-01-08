from backend.grammar import evaluate_new_query

TEST_CASES = {
    "simple_percentile": {
        "query": "pp(0.5;ge;0.5)",
        "expected": {0, 1, 2, 3},
    },
    "percentile_and_keyword": {
        "query": "pp(0.5;ge;0.5) AND kw(test)",
        "expected": {1, 2},
    },
    "complex_operation": {
        "query": "pp(0.5;ge;0.5) AND (kw(test) OR kw(other))",
        "expected": {1, 2, 3},
    },
    "not_operation": {
        "query": "NOT pp(0.5;ge;0.5)",
        "expected": {4, 5},
    },
    "keyword_only": {
        "query": "kw(test)",
        "expected": {1, 2, 3},
    },
}

def test_grammar():
    """Test the new grammar implementation"""
    for test_name, test_case in TEST_CASES.items():
        result = evaluate_new_query(test_case["query"])
        assert set(result) == test_case["expected"], \
            f"Test '{test_name}' failed. Expected {test_case['expected']}, got {set(result)}"
