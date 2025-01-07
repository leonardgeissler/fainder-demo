import time

import pytest
from loguru import logger

from backend.percentile_grammar import evaluate_new_query

# TODO: Add more test cases and complex queries

queries = [
    "kw(germany)",
    "kw(germany) AND pp(0.5;ge;20.0)",
    "kw(germany or TMDB)",
    "pp(0.5;ge;5000)",
    "pp(0.9;ge;1000000)",
    "pp(0.9;ge;1000000) AND kw(germany)",
    "pp(0.9;ge;1000000) OR kw(germany)",
    "pp(0.9;ge;1000000) AND kw(a)",
]

expected_results = [[0], [0], [0, 2], [1, 2], [2], [], [0, 2], [2]]


# Test the new grammar correctness
@pytest.mark.parametrize("query, expected_result", zip(queries, expected_results, strict=False))
def test_new_grammar_correctness(query: str, expected_result: list[int]) -> None:
    start = time.perf_counter()
    result = evaluate_new_query(query)
    end = time.perf_counter()
    assert result == expected_result
    time_taken_1 = end - start

    start = time.perf_counter()
    result = evaluate_new_query(query, enable_result_caching=False)
    end = time.perf_counter()
    assert result == expected_result
    time_taken_2 = end - start

    logger.info(f"Time taken with filter: {time_taken_1} and without filter: {time_taken_2}")
    assert time_taken_1 > time_taken_2
