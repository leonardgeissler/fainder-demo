import time
from typing import Any

import pytest
from loguru import logger

from backend.grammar import evaluate_query

TEST_CASES = {
    "simple_percentile": {"query": "0.5;ge;5000", "expected": [1, 2]},
    "high_percentile": {"query": "0.9;ge;1000000", "expected": [1, 2]},
    "percentile_and_percentile": {"query": "0.9;ge;1000000 AND 0.5;ge;20.0", "expected": [1, 2]},
    "percentile_or_percentile": {"query": "0.9;ge;1000000 OR 0.5;ge;20.0", "expected": [0, 1, 2]},
    "percentile_xor_percentile": {"query": "0.9;ge;1000000 XOR 0.5;ge;20.0", "expected": [0]},
    "not_percentile": {"query": "NOT 0.9;ge;1000000", "expected": [0]},
    "not_complex": {"query": "NOT (0.9;ge;1000000 AND 0.5;ge;20.0)", "expected": [0]},
}


@pytest.mark.parametrize(("test_name", "test_case"), TEST_CASES.items())
def test_old_grammar_correctness(test_name: str, test_case: dict[str, Any]) -> None:
    query = test_case["query"]
    expected_result = test_case["expected"]

    start = time.perf_counter()
    result = evaluate_query(query)
    end = time.perf_counter()
    time_taken = end - start
    logger.info(f"Result: {result}")

    assert set(expected_result) == result
    logger.info(f"Time taken: {time_taken}")
