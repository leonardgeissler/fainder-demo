# type: ignore
# Description: This file contains the grammer for the percentile query language.


from typing import List, Set, Union, Literal
from lark import Lark, Token, Transformer, Tree
from fainder.typing import PercentileQuery
from fainder_demo.config import INDEX, LIST_OF_HIST, LIST_OF_DOCS, METADATA
from fainder.execution.runner import run
from numpy import uint32
from loguru import logger



def number_of_matching_histograms_to_doc_number(matching_histograms: set[uint32]) -> list[int]:
    """
    This function will take a set of histogram ids and return a list of document ids.
    """

    doc_ids = set()
    for i in matching_histograms:
        split = LIST_OF_HIST[i].split("&")
        doc_id = split[2] 
        doc_ids.add(LIST_OF_DOCS.index(doc_id))
    return list(doc_ids)

def get_histogram_ids_from_identifer(identifer: str) -> set[uint32]:
    """
    This function will take a column name and return a set of histogram ids.
    """
    column_names: dict[str,list[str]]=METADATA["column_names"] 
    histogram_ids = set()
    try:
        histogram_strings = column_names[identifer]
    except KeyError:
        return histogram_ids
    for hist_str in histogram_strings:
        histogram_ids.add(LIST_OF_HIST.index(hist_str))
    return histogram_ids


def run_percentile(query: str, filter_hist: set[uint32] | None = None) -> list[int]:
    """
    This function will run the percentile query.
    Example input: 0.5;ge;20.2;age 
    Example output: set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    """
    if len(filter_hist) == 0:
        return []
    
    split_query = query.split(";")
    assert len(split_query) == 3 or len(split_query) == 4

    percentile = float(split_query[0])
    assert 0 < percentile <= 1

    reference = float(split_query[2])

    assert split_query[1] in ["ge", "gt", "le", "lt"]
    comparison: Literal["le", "lt", "ge", "gt"] = split_query[1] # type: ignore
    
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

    q: PercentileQuery =  (percentile, comparison, reference)

    print(f"Filter histograms: {filter_histograms}")

    print(f"Filter length: {len(filter_histograms)}") 

    result = run(INDEX, [q], "index", hist_filter=filter_histograms) 

    matching_histograms = result[0]
    logger.info(f"Matching histograms: {matching_histograms}")

    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])

    return matching_docs


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
    return Lark(grammar, start='start')

class QueryEvaluator(Transformer):
    def __init__(self):
        self.filter_hist: set[uint32] | None = None

    def term(self, items: List[Token]) -> Set[int]:
        term_str = ";".join(item.value for item in items)
        return set(run_percentile(term_str, self.filter_hist))

    def expression(self, items: List[Union[Set[int], Tree]]) -> Set[int]:
        if len(items) == 1:
            return items[0]
        return items[0]  # Single expressions are passed as-is.

    def query(self, items: List[Union[Set[int], Token]]) -> Set[int]:
        left = items[0]
        if len(items) == 3:
            operator = items[1].value.strip()
            right = items[2]

            if operator == "AND":
                return left & right  # Intersection 
            elif operator == "OR":
                return left | right  # Union # TODO: make this more efficient
            elif operator == "XOR":
                return left ^ right  # Symmetric Difference
        return left

    def start(self, items: List[Set[int]]) -> Set[int]:
        return items[0]

GRAMMAR = build_grammar()
EVALUATOR = QueryEvaluator()

# Evaluate the query.
def evaluate_query(query: str, filter_hist: set[uint32] | None = None) -> Set[int]:
    EVALUATOR.filter_hist = filter_hist
    tree = GRAMMAR.parse(query)
    return EVALUATOR.transform(tree)

