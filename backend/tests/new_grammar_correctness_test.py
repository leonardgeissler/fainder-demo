import time

import pytest
from loguru import logger

from backend.grammar import evaluate_new_query

# TODO: Add more test cases and complex queries

queries = [
    "kw(germany)",
    "kw(germany) AND pp(0.5;ge;20.0)",
    "kw(germany OR TMDB)",
    "pp(0.5;ge;5000)",
    "pp(0.9;ge;1000000)",
    "pp(0.9;ge;1000000) AND kw(germany)",
    "pp(0.9;ge;1000000) OR kw(germany)",
    "pp(0.9;ge;1000000) AND kw(a)",
    "kw(a)",
]

expected_results = [[0], [0], [2, 0], [1, 2], [1, 2], [], [0, 1, 2], [2, 1], [2, 1, 0]]


# Test the new grammar correctness
@pytest.mark.parametrize("query, expected_result", zip(queries, expected_results, strict=False))
def test_new_grammar_correctness(query: str, expected_result: list[int]) -> None:
    start = time.perf_counter()
    result1 = evaluate_new_query(query)
    end = time.perf_counter()
    time_taken_1 = end - start
    logger.info(f"Result1: {result1}")

    start = time.perf_counter()
    result2 = evaluate_new_query(query, enable_result_caching=False)
    end = time.perf_counter()
    time_taken_2 = end - start
    logger.info(f"Result2: {result2}")

    assert result1 == result2
    assert expected_result == result1

    logger.info(f"Time taken with filter: {time_taken_1} and without filter: {time_taken_2}")
    div = time_taken_1 - time_taken_2
    logger.info(f"Time difference: {div}")
