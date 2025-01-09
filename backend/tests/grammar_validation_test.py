import pytest
from lark import exceptions

from backend.grammar import evaluate_query

test_queries = {
    "valid": [
        "0.5;ge;20.2;age",
        "0.5;ge;20",
        "(0.5;ge;20) AND (0.5;le;200 OR 0.5;ge;200)",
        "0.5;ge;20 AND 0.5;le;200 OR 0.5;ge;200",
        "0.5;ge;20 AND 0.5;le;5;Month OR NOT 0.5;ge;200",
    ],
    "invalid": [
        "0.5;ge;age",
        "0.5;20;20",
        "(0.5;ge;20) AND",
        "0.5;ge;20 AND 0.5;le;200 OR",
        "0.5;ge;20 AND 0.5;le;5;Month OR NOT 0.5;ge;200 AND",
        "0.5;ge;20 AND 0.5;le;5;Month OR NOT (0.5;ge;200",
    ],
}


@pytest.mark.parametrize("query", test_queries["valid"])
def test_query_evaluation_success(query: str) -> None:
    r = evaluate_query(query)
    assert not isinstance(r, Exception)


@pytest.mark.parametrize("query", test_queries["invalid"])
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
        evaluate_query(query)
