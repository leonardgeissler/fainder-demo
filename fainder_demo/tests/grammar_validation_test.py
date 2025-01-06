from fainder_demo.percentile_grammar import evaluate_query

from parameterized import parameterized
import unittest 

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

    @parameterized.expand(testcases)
    def testExsampleWorks(self, query: str) -> None:
        r = evaluate_query(query)
        self.assertNotIsInstance(r, Exception)

    @parameterized.expand(testcases_expect_reject)
    def testFail(self, query) -> None:
        with self.assertRaises(Exception):
            evaluate_query(query)

if __name__ == "__main__":
    unittest.main()