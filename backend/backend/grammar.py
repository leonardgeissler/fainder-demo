import os
import time
from collections.abc import Sequence
from typing import Literal

import grpc
from fainder.execution.runner import run
from fainder.typing import PercentileQuery
from lark import Lark, Token, Transformer, Tree, Visitor
from loguru import logger
from numpy import uint32

from backend.config import INDEX, LIST_OF_DOCS
from backend.proto.keyword_query_pb2 import QueryRequest  # type: ignore
from backend.proto.keyword_query_pb2_grpc import KeywordQueryStub
from backend.utils import (
    get_histogram_ids_from_identifer,
    number_of_matching_histograms_to_doc_number,
)

# TODO: All the code in here should be class-based: We need a LuceneConnector class and a
# QueryEvaluator class


def call_lucene_server(
    query: str, doc_ids: list[int] | None = None
) -> tuple[Sequence[int], Sequence[float] | None]:
    """
    Calls the Lucene server to evaluate a keyword query and retrieve document IDs.

    Args:
        query: The query string to be evaluated by Lucene.
        doc_ids: A list of document IDs to consider as a filter (none by default).

    Returns:
        list[int]: A list of document IDs that match the query.
        list[float]: A list of scores for each document ID (if available).

    Raises:
        grpc.RpcError: If there is an error while calling the Lucene server.
    """
    # TODO: Redesign this function with a LuceneConnector class so that we do
    # not have to reinitialize the connection every time we want to evaluate a Lucene query
    # TODO: Use the ranking returned by Lucene to sort the results
    # TODO: This functionality should be moved to a separate module outside of the grammar
    start = time.perf_counter()

    lucene_host = os.getenv("LUCENE_HOST", "127.0.0.1")
    lucene_port = os.getenv("LUCENE_PORT", "8001")
    try:
        logger.debug(f"Executing query: '{query}' with filter: {doc_ids}")
        with grpc.insecure_channel(f"{lucene_host}:{lucene_port}") as channel:
            stub = KeywordQueryStub(channel)
            response = stub.Evaluate(QueryRequest(query=query, doc_ids=doc_ids or []))
            result = response.results

        logger.debug(f"Lucene query execution took {time.perf_counter() - start:.3f} seconds")
        logger.debug(f"Keyword query result: {result}")

        return response.results, response.scores
    except grpc.RpcError as e:
        logger.error(f"Calling Lucene raised an error: {e}")
        return [], []


def run_keyword(
    query: str, filter_doc_ids: list[int]
) -> tuple[Sequence[int], Sequence[float] | None]:
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


class OperatorVisitor(Visitor):
    def __init__(self):
        self.current_operator = None
        self.current_side = None

    def query(self, tree):
        if len(tree.children) == 3:  # Has operator
            old_operator = self.current_operator
            old_side = self.current_side

            self.current_operator = tree.children[1].value

            # Visit left side
            self.current_side = "left"
            self.visit(tree.children[0])

            # Visit right side
            self.current_side = "right"
            self.visit(tree.children[2])

            self.current_operator = old_operator
            self.current_side = old_side
        else:
            self.visit(tree.children[0])

    def percentileterm(self, tree):
        if self.current_operator:
            logger.debug(tree)
            logger.debug(tree.children)

            tree.children.append(self.current_operator)
            tree.children.append(self.current_side)

    def keywordterm(self, tree):
        if self.current_operator:
            logger.debug(tree)
            logger.debug(tree.children)
            tree.children.append(self.current_operator)
            tree.children.append(self.current_side)


class NewQueryEvaluator(Transformer):
    def __init__(self, enable_result_caching: bool = True):
        logger.debug("Initializing NewQueryEvaluator")
        self.scores: dict[int, float] = {}
        self.last_result: set[int] | None = None
        self.current_side: str | None = None
        self.enable_result_caching = enable_result_caching

    def _get_filter_docs(self, operator: str | None, side: str | None) -> list[int] | None:
        """
        Determine which documents to filter based on operator and evaluation side
        Returns None if no filter is needed
        """
        if not self.enable_result_caching:
            return None
        if not operator or not self.last_result or not side:
            return None

        # Only apply filter for right side of AND operations
        if operator == "AND" and side == "right":
            logger.debug(f"Applying filter from previous result: {self.last_result}")
            return list(self.last_result)

        return None

    def add_scores(self, doc_ids: list[int], scores: list[float] | None) -> None:
        logger.debug(f"Adding scores for {len(doc_ids) if doc_ids else 0} documents")
        if not scores:
            return
        for i, doc_id in enumerate(doc_ids):
            self.scores[doc_id] = scores[i]

    def percentileterm(self, items: list[Token]) -> set[int]:
        # Keep track of seen pairs to avoid duplicates
        seen_operator_side = False
        operator = None
        side = None
        term_items = []

        # Process items to extract term components and first operator/side pair
        for item in items:
            if isinstance(item, Token):
                term_items.append(item)
            elif not seen_operator_side:
                # First occurrence of operator/side pair
                operator = items[len(term_items)]
                side = items[len(term_items) + 1]
                seen_operator_side = True
                break

        logger.debug(f"Term items: {term_items}")
        term_str = ";".join(item.value for item in term_items)

        logger.debug(f"Evaluating percentile term: {term_str} with operator {operator} on {side}")
        result = set(run_percentile(term_str, None))
        self.last_result = result
        return result

    def keywordterm(self, items: list[Token]) -> set[int]:
        keyword = items[0].value.strip()
        operator = items[-2] if len(items) > 2 else None
        side = items[-1] if len(items) > 2 else None

        logger.debug(f"Evaluating keyword term: {keyword} with operator {operator} on {side}")
        filter_docs = self._get_filter_docs(operator, side)

        result, scores = run_keyword(keyword, filter_docs)
        self.add_scores(result, scores)
        result_set = set(result)
        self.last_result = result_set
        return result_set

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
        logger.debug(
            f"Query operator: {operator}, left size: {len(left)}, right size: {len(right)}"
        )

        result = None
        if operator == "AND":
            result = left & right
        elif operator == "OR":
            result = left | right
        else:  # XOR
            result = left ^ right

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
    tree = NEW_GRAMMAR.parse(query)

    operator_visitor = OperatorVisitor()
    operator_visitor.visit(tree)

    logger.debug(f"Tree: {tree.pretty()}")

    evaluator = NewQueryEvaluator(enable_result_caching)

    result: list[int] = list(evaluator.transform(tree))

    if evaluator.scores:
        # Sort the results by score if scores are available. High scores first
        # If a score is not available, then push it to the end
        result.sort(key=lambda x: evaluator.scores.get(x, -1), reverse=True)
    return result
