import os
import time
from typing import Literal

import requests
from fainder.execution.runner import run
from fainder.typing import PercentileQuery
from lark import Lark, Token, Transformer, Tree
from loguru import logger
from numpy import uint32

from backend.config import INDEX, LIST_OF_DOCS
from backend.utils import (
    get_histogram_ids_from_identifer,
    number_of_matching_histograms_to_doc_number,
)


def call_lucene_server(keywords: str) -> list[int]:
    """
    This function will call the lucene server and return the results.
    """
    start = time.perf_counter()

    lucene_host = os.getenv("LUCENE_HOST", "127.0.0.1")
    lucene_port = os.getenv("LUCENE_PORT", "8001")
    url = f"http://{lucene_host}:{lucene_port}/search"
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json={"keywords": keywords}, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()

        logger.debug(f"Raw Lucene response: {response.text}")
        array: list[int] = data["results"]

        # verify is array of integers
        assert all(isinstance(x, int) for x in array)

        logger.info(f"Lucene server took {time.perf_counter() - start} seconds")
        logger.debug(f"Lucene results: {array}")

        return array
    except Exception as e:
        logger.error(f"Error calling Lucene server: {e}")
        logger.error(
            f"Response content: {response.text if 'response' in locals() else 'No response'}"
        )
        return []


def run_keyword(query: str) -> list[int]:
    """
    This function will run the keyword query on the lucene server.
    """
    return call_lucene_server(query)


def run_percentile(query: str, filter_hist: set[uint32] | None = None) -> list[int]:
    """
    This function will run the percentile query.
    Example input: 0.5;ge;20.2;age
    Example output: set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    """
    if filter_hist and len(filter_hist) == 0:
        return []

    split_query = query.split(";")
    assert len(split_query) == 3 or len(split_query) == 4

    percentile = float(split_query[0])
    assert 0 < percentile <= 1

    reference = float(split_query[2])

    assert split_query[1] in ["ge", "gt", "le", "lt"]
    comparison: Literal["le", "lt", "ge", "gt"] = split_query[1]  # type: ignore

    split_query = query.split(";")
    indentifer = None
    filter_histograms: None | set[uint32] = filter_hist
    if len(split_query) == 4:
        indentifer = split_query[3]
        add_filter_histograms = get_histogram_ids_from_identifer(indentifer)
        if filter_hist is None or len(filter_hist) == 0:
            filter_histograms = add_filter_histograms
        else:
            filter_histograms = filter_hist & add_filter_histograms
        if len(filter_histograms) == 0:
            return []

    q: PercentileQuery = (percentile, comparison, reference)

    print(f"Filter histograms: {filter_histograms}")

    result = run(INDEX, [q], "index", hist_filter=filter_histograms)

    matching_histograms = result[0]
    logger.info(f"Matching histograms: {matching_histograms}")

    return number_of_matching_histograms_to_doc_number(matching_histograms[0])


def build_grammar() -> Lark:
    grammar = """
    start: query
    query: expression (OPERATOR query)?
    expression: term | "NOT" expression | "(" query ")"
    term: NUMBER ";" COMPARISON ";" NUMBER ";" IDENTIFIER
        | NUMBER ";" COMPARISON ";" NUMBER
    OPERATOR: "AND" | "OR" | "XOR"
    COMPARISON: "ge" | "gt" | "le" | "lt"
    NUMBER: /[0-9]+(\\.[0-9]+)?/
    IDENTIFIER: /[a-zA-Z0-9_]+/
    %ignore " "
    """
    return Lark(grammar, start="start")


class QueryEvaluator(Transformer):
    def __init__(self):
        self.filter_hist: set[uint32] | None = None

    def get_all_doc_ids(self) -> set[int]:
        """Returns a set of all possible document IDs"""
        return set(range(len(LIST_OF_DOCS)))

    def term(self, items: list[Token]) -> set[int]:
        term_str = ";".join(item.value for item in items)
        return set(run_percentile(term_str, self.filter_hist))

    def expression(self, items: list[set[int] | Tree]) -> set[int]:
        if len(items) == 1:
            return items[0]
        if isinstance(items[0], Token) and items[0].value == "NOT":
            # Handle NOT by returning the complement of the expression
            return self.get_all_doc_ids() - items[1]
        return items[0]  # Single expressions are passed as-is

    def query(self, items: list[set[int] | Token]) -> set[int]:
        left = items[0]
        if len(items) == 3:
            operator = items[1].value.strip()
            right = items[2]

            if operator == "AND":
                return left & right  # Intersection
            if operator == "OR":
                return left | right  # Union # TODO: make this more efficient
            if operator == "XOR":
                return left ^ right  # Symmetric Difference
        return left

    def start(self, items: list[set[int]]) -> set[int]:
        return items[0]


GRAMMAR = build_grammar()
EVALUATOR = QueryEvaluator()


# Evaluate the query.
def evaluate_query(query: str, filter_hist: set[uint32] | None = None) -> set[int]:
    EVALUATOR.filter_hist = filter_hist
    tree = GRAMMAR.parse(query)
    return EVALUATOR.transform(tree)


def build_new_grammar() -> Lark:
    grammar = """
    start: query
    query: expression (OPERATOR query)?
    expression: ("pp(" | "percentile(") percentileterm ")"| "NOT" expression
        | "(" query ")" | ("kw(" | "keyword(") keywordterm ")"
    percentileterm: NUMBER ";" COMPARISON ";" NUMBER ";" IDENTIFIER
        | NUMBER ";" COMPARISON ";" NUMBER
    keywordterm: KEYWORD
    OPERATOR: "AND" | "OR" | "XOR"
    COMPARISON: "ge" | "gt" | "le" | "lt"
    NUMBER: /[0-9]+(\\.[0-9]+)?/
    IDENTIFIER: /[a-zA-Z0-9_]+/
    KEYWORD: /[^;)]+/
    %ignore " "
    """
    return Lark(grammar, start="start")


class NewQueryEvaluator(Transformer):
    def __init__(self):
        self.filter_hist: set[uint32] | None = None

    def get_all_doc_ids(self) -> set[int]:
        """Returns a set of all possible document IDs"""
        return set(range(len(LIST_OF_DOCS)))

    def percentileterm(self, items: list[Token]) -> set[int]:
        term_str = ";".join(item.value for item in items)
        return set(run_percentile(term_str, self.filter_hist))

    def keywordterm(self, items: list[Token]) -> set[int]:
        # Extract the keyword search term
        keyword = items[0].value.strip()
        return set(run_keyword(keyword))

    def expression(self, items: list[set[int] | Tree | Token]) -> set[int]:
        if len(items) == 1:
            return items[0]
        if isinstance(items[0], Token):
            if items[0].value == "NOT":
                return self.get_all_doc_ids() - items[1]
            if items[0].value in ["pp(", "percentile(", "kw(", "keyword("]:
                return items[1]
        return items[0]

    def query(self, items: list[set[int] | Token]) -> set[int]:
        left = items[0]
        if len(items) == 3:
            operator = items[1].value.strip()
            right = items[2]

            if operator == "AND":
                return left & right
            if operator == "OR":
                return left | right
            if operator == "XOR":
                return left ^ right
        return left

    def start(self, items: list[set[int]]) -> set[int]:
        return items[0]


NEW_GRAMMAR = build_new_grammar()
NEW_EVALUATOR = NewQueryEvaluator()


def evaluate_new_query(query: str, filter_hist: set[uint32] | None = None) -> set[int]:
    NEW_EVALUATOR.filter_hist = filter_hist
    tree = NEW_GRAMMAR.parse(query)
    return NEW_EVALUATOR.transform(tree)
