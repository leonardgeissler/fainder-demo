import pytest

from backend.percentile_grammar import evaluate_new_query

# TODO: Add more test cases

queries = [
    "kw(germany)" "kw(germany) AND pp(0.5;ge;20.0)",
    "kw(germany or TMDB)",
    "pp(0.5;ge;5000)",
]

expected_results = [[0], [0], [0, 2], [1, 2]]


# Test the new grammar correctness
@pytest.mark.parametrize("query, expected_result", zip(queries, expected_results, strict=False))
def test_new_grammar_correctness(query: str, expected_result: list[int]) -> None:
    result = evaluate_new_query(query)
    assert result == expected_result
