import pytest
from lark import exceptions

from backend.percentile_grammar import evaluate_new_query

correct_queries = [
    # Basic keyword queries
    "keyword(test)",
    "kw(hello world)",
    # Basic percentile queries
    "pp(0.5;ge;20.0)",
    "percentile(0.5;ge;20.0;age)",
    # Combined queries
    "kw(test) AND pp(0.5;ge;20.0)",
    "keyword(hello) OR percentile(0.5;ge;20.0)",
    "kw(test) XOR pp(0.5;ge;20.0)",
    # Nested queries
    "(kw(test) AND pp(0.5;ge;20.0)) OR keyword(other)",
    "(keyword(hello) OR kw(world)) AND pp(0.5;ge;20.0)",
    # NOT operations
    "NOT kw(test)",
    "NOT pp(0.5;ge;20.0)",
    "NOT (kw(test) AND pp(0.5;ge;20.0))",
]

wrong_queries = [
    # Invalid syntax
    "keyword()",
    "pp()",
    "kw()",
    # Missing parentheses
    "keyword(test",
    "pp(0.5;ge;20.0",
    # Invalid operators
    "kw(test) INVALID pp(0.5;ge;20.0)",
    "kw(test) AND OR pp(0.5;ge;20.0)",
    # Incomplete expressions
    "kw(test) AND",
    "NOT",
    # Invalid percentile queries
    "pp(a;ge;20.0)",
    "pp(0.5;invalid;20.0)",
    "pp(0.5;ge;abc)",
    # Malformed compound queries
    "(kw(test) AND",
    "kw(test)) OR pp(0.5;ge;20.0)",
    "AND kw(test)",
]


@pytest.mark.parametrize("query", correct_queries)
def test_query_evaluation_success(query: str) -> None:
    r = evaluate_new_query(query)
    assert not isinstance(r, Exception)


@pytest.mark.parametrize("query", wrong_queries)
def test_query_evaluation_fail(query: str) -> None:
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
