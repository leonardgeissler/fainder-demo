import os
import time
from typing import Any, Literal

import requests
from fainder.execution.runner import run
from fainder.typing import PercentileQuery
from lark import Lark, Token, Transformer, Tree, v_args
from loguru import logger
from numpy import uint32

from backend.config import INDEX, LIST_OF_DOCS
from backend.utils import (
    get_histogram_ids_from_identifer,
    get_hists_for_doc_ids,
    number_of_matching_histograms_to_doc_number,
)

# TODO: All the code in here should be class-based: We need a LuceneConnector class and a
# QueryEvaluator class


def call_lucene_server(
    keywords: str, filter_doc_ids: list[int] | None = None
) -> tuple[list[int], list[float] | None]:
    """
    This function will call the lucene server and return the results.
    """
    # TODO: Redesign this function with gRPC and implement a LuceneConnector class so that we do
    # not have to reinitialize the connection every time we want to evaluate a Lucene query
    # TODO: Use the ranking returned by Lucene to sort the results
    # TODO: This functionality should be moved to a separate module outside of the grammar
    logger.debug(f"Calling Lucene server with keywords: {keywords} and filter: {filter_doc_ids}")
    start = time.perf_counter()

    lucene_host = os.getenv("LUCENE_HOST", "127.0.0.1")
    lucene_port = os.getenv("LUCENE_PORT", "8001")
    url = f"http://{lucene_host}:{lucene_port}/search"
    headers = {"Content-Type": "application/json"}
    try:
        json_data: dict[str, Any] = {"keywords": keywords}
        if filter_doc_ids:
            json_data["filter"] = filter_doc_ids
        response = requests.post(url, json=json_data, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()

        logger.debug(f"Raw Lucene response: {response.text}")
        array: list[int] = data["results"]
        scores: list[float] | None = None
        if "scores" in data:
            scores = data["scores"]

        # verify is array of integers
        # NOTE: This part is irrevelant once we move to protobuf since it has a guaranteed schema
        assert all(isinstance(x, int) for x in array)

        logger.info(f"Lucene server took {time.perf_counter() - start} seconds")
        logger.debug(f"Lucene results: {array}")

        return array, scores
    except requests.RequestException as e:
        logger.error(f"Calling Lucene server failed: {e}")
        return ([], [])


def run_keyword(query: str, filter_doc_ids: list[int]) -> tuple[list[int], list[float] | None]:
    """
    This function will run the keyword query on the lucene server.
    """
    # TODO: Should be part of the LuceneConnector class
    return call_lucene_server(query, filter_doc_ids)


def run_percentile(query: str, filter_hist: set[uint32] | None = None) -> list[int]:
    """
    This function will run the percentile query.
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
    expression: not_expr | term | "(" query ")"
    not_expr: "NOT" term | "NOT" "(" query ")"
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

    def not_expr(self, items: list[set[int] | Tree | Token]) -> set[int]:
        logger.debug("Evaluating NOT expression")
        # Get the result to negate (could be a term or query result)
        to_negate = items[0] if isinstance(items[0], set) else items[1]
        result = self.get_all_doc_ids() - to_negate
        logger.debug(f"NOT result size: {len(result)}")
        return result

    def expression(self, items: list[set[int] | Tree]) -> set[int]:
        return items[0]  # Simply return the result, NOT is handled in not_expr

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
    expression: not_expr | term | "(" query ")"
    not_expr: "NOT" term | "NOT" "(" query ")"
    term: ("pp(" | "percentile(") percentileterm ")" | ("kw(" | "keyword(") keywordterm ")"
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
    def __init__(self, enable_result_caching: bool = True):
        logger.debug("Initializing NewQueryEvaluator")
        self.scores: dict[int, float] = {}

    def add_scores(self, doc_ids: list[int], scores: list[float] | None) -> None:
        logger.debug(f"Adding scores for {len(doc_ids) if doc_ids else 0} documents")
        if not scores:
            return
        for i, doc_id in enumerate(doc_ids):
            self.scores[doc_id] = scores[i]

    def percentileterm(self, items: list[Token]) -> set[int]:
        term_str = ";".join(item.value for item in items)
        logger.debug(f"Evaluating percentile term: {term_str}")
        result = set(run_percentile(term_str, None))
        logger.debug(f"Percentile term result: {result}")
        return result

    def keywordterm(self, items: list[Token]) -> set[int]:
        keyword = items[0].value.strip()
        logger.debug(f"Evaluating keyword term: {keyword}")
        result, scores = run_keyword(keyword, [])
        self.add_scores(result, scores)
        logger.debug(f"Keyword term result: {result}")
        return set(result)

    def term(self, items: list[set[int] | Tree | Token]) -> set[int]:
        logger.debug(f"Evaluating term with items: {items}")
        if isinstance(items[0], Token):
            if items[0].value in ["pp(", "percentile("]:
                return items[1]
            if items[0].value in ["kw(", "keyword("]:
                return items[1]
        return items[0]

    def not_expr(self, items: list[set[int] | Tree | Token]) -> set[int]:
        logger.debug(f"Evaluating NOT expression with {len(items)} items")
        # Get the result to negate (could be a term or query result)
        to_negate = items[0] if isinstance(items[0], set) else items[1]
        all_docs = self.get_all_doc_ids()
        result = all_docs - to_negate
        logger.debug(f"NOT expression result size: {len(result)}")
        return result

    def expression(self, items: list[set[int] | Tree | Token]) -> set[int]:
        logger.debug(f"Evaluating expression with {len(items)} items")
        return items[0]

    def query(self, items: list[set[int] | Token]) -> set[int]:
        logger.debug(f"Evaluating query with {len(items)} items")
        if len(items) == 1:
            return items[0]

        left = items[0]
        operator = items[1].value.strip()
        right = items[2]
        logger.debug(f"Query operator: {operator}, left size: {len(left)}, right size: {len(right)}")

        result = None
        if operator == "AND":
            result = left & right
        elif operator == "OR":
            result = left | right
        else:  # XOR
            result = left ^ right
        
        logger.debug(f"Query result size: {len(result)}")
        return result

    def start(self, items: list[set[int]]) -> set[int]:
        logger.debug("Starting query evaluation")
        result = items[0]
        logger.debug(f"Final result size: {len(result)}")
        return result

    def get_all_doc_ids(self) -> set[int]:
        logger.debug("Getting all document IDs")
        return set(range(len(LIST_OF_DOCS)))


NEW_GRAMMAR = build_new_grammar()


def evaluate_new_query(query: str, enable_result_caching: bool = True) -> list[int]:
    evaluator = NewQueryEvaluator(enable_result_caching)
    tree = NEW_GRAMMAR.parse(query)
    result: list[int] = list(evaluator.transform(tree))

    if evaluator.scores:
        # Sort the results by score if scores are available. High scores first
        # If a score is not available, then push it to the end
        result.sort(key=lambda x: evaluator.scores.get(x, -1), reverse=True)
    return result
