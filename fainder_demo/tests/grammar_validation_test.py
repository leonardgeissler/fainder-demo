import unittest

from lark import exceptions
from parameterized import parameterized

from fainder_demo.percentile_grammar import evaluate_query

testcases = [
    "0.5;ge;20.2;age",
    "0.5;ge;20",
    "(0.5;ge;20) AND (0.5;le;200 OR 0.5;ge;200)",
    "0.5;ge;20 AND 0.5;le;200 OR 0.5;ge;200",
    "0.5;ge;20 AND 0.5;le;5;Month OR NOT 0.5;ge;200",
]

testcases_expect_reject = [
    "0.5;ge;age",
    "0.5;20;20",
    "(0.5;ge;20) AND",
    "0.5;ge;20 AND 0.5;le;200 OR",
    "0.5;ge;20 AND 0.5;le;5;Month OR NOT 0.5;ge;200 AND",
    "0.5;ge;20 AND 0.5;le;5;Month OR NOT (0.5;ge;200",
]


class TestQuery(unittest.TestCase):
    @parameterized.expand(testcases)  # type: ignore[misc]
    def test_query_evaluation_success(self, query: str) -> None:
        r = evaluate_query(query)
        self.assertNotIsInstance(r, Exception)

    @parameterized.expand(testcases_expect_reject)  # type: ignore[misc]
    def test_query_evaluation_fail(self, query: str) -> None:
        with self.assertRaises(
            (
                exceptions.UnexpectedCharacters,
                exceptions.UnexpectedToken,
                exceptions.UnexpectedInput,
                exceptions.UnexpectedEOF,
                SyntaxError,
            )
        ):
            evaluate_query(query)


if __name__ == "__main__":
    unittest.main()
