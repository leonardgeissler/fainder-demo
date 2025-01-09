import pytest
from lark import exceptions

from backend.grammar import evaluate_new_query

VALID_TEST_CASES = {
    "basic_keyword": {
        "queries": [
            "keyword(test)",
            "kw(hello world)",
        ]
    },
    "basic_percentile": {
        "queries": [
            "pp(0.5;ge;20.0)",
            "percentile(0.5;ge;20.0;age)",
        ]
    },
    "combined": {
        "queries": [
            "kw(test) AND pp(0.5;ge;20.0)",
            "keyword(hello) OR percentile(0.5;ge;20.0)",
            "kw(test) XOR pp(0.5;ge;20.0)",
        ]
    },
    "nested": {
        "queries": [
            "(kw(test) AND pp(0.5;ge;20.0)) OR keyword(other)",
            "(keyword(hello) OR kw(world)) AND pp(0.5;ge;20.0)",
        ]
    },
    "not_operations": {
        "queries": [
            "NOT kw(test)",
            "NOT pp(0.5;ge;20.0)",
            "NOT (kw(test) AND pp(0.5;ge;20.0))",
        ]
    },
}

INVALID_TEST_CASES = {
    "invalid_syntax": {
        "queries": [
            "keyword()",
            "pp()",
            "kw()",
        ]
    },
    "missing_parentheses": {
        "queries": [
            "keyword(test",
            "pp(0.5;ge;20.0",
        ]
    },
    "invalid_operators": {
        "queries": [
            "kw(test) INVALID pp(0.5;ge;20.0)",
            "kw(test) AND OR pp(0.5;ge;20.0)",
        ]
    },
    "incomplete_expressions": {
        "queries": [
            "kw(test) AND",
            "NOT",
        ]
    },
    "invalid_percentile": {
        "queries": [
            "pp(a;ge;20.0)",
            "pp(0.5;invalid;20.0)",
            "pp(0.5;ge;abc)",
        ]
    },
    "malformed_compound": {
        "queries": [
            "(kw(test) AND",
            "kw(test)) OR pp(0.5;ge;20.0)",
            "AND kw(test)",
        ]
    },
}


@pytest.mark.parametrize(
    ("category", "query"),
    [(cat, q) for cat, data in VALID_TEST_CASES.items() for q in data["queries"]],
)
def test_query_evaluation_success(category: str, query: str) -> None:
    r = evaluate_new_query(query)
    assert not isinstance(r, Exception)


@pytest.mark.parametrize(
    ("category", "query"),
    [(cat, q) for cat, data in INVALID_TEST_CASES.items() for q in data["queries"]],
)
def test_query_evaluation_fail(category: str, query: str) -> None:
    with pytest.raises(
        (
            exceptions.UnexpectedCharacters,
            exceptions.UnexpectedToken,
            exceptions.UnexpectedInput,
            exceptions.UnexpectedEOF,
            SyntaxError,
        )
    ):
        evaluate_new_query(query)
