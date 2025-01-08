from typing import Any

from fainder.execution.runner import run
from loguru import logger

from backend.config import INDEX, LIST_OF_DOCS
from backend.grammar import call_lucene_server, evaluate_new_query, evaluate_query
from backend.utils import (
    get_hists_for_doc_ids,
    get_metadata,
    number_of_matching_histograms_to_doc_number,
    parse_precentile_query,
)


def search_basic(percentile: str, keywords: str) -> list[dict[str, Any]]:
    results_keywords = call_lucene_server(keywords)

    if len(results_keywords) == 0:
        return []

    filter_hist = get_hists_for_doc_ids(results_keywords)
    matching_docs = list(evaluate_query(percentile, filter_hist))
    return get_metadata(matching_docs)


def search_combined(percentile: str, keywords: str) -> list[dict[str, Any]]:
    results_keywords = call_lucene_server(keywords)
    matching_docs = list(evaluate_query(percentile))
    combined_results = [i for i in results_keywords if i in matching_docs]
    return get_metadata(combined_results)


def search_single(percentile: str, keywords: str) -> list[dict[str, Any]]:
    results_keywords = call_lucene_server(keywords)
    query = parse_precentile_query(percentile)
    result = run(INDEX, [query], "index")
    matching_histograms = result[0]
    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])
    combined_results = [i for i in results_keywords if i in matching_docs]
    return get_metadata(combined_results)


def query_bit_array(percentile: str) -> list[int]:
    query = parse_precentile_query(percentile)
    result = run(INDEX, [query], "index")
    matching_histograms = result[0]
    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])
    return [1 if i in matching_docs else 0 for i in range(len(LIST_OF_DOCS))]


def search(query: str) -> list[dict[str, Any]]:
    result = evaluate_new_query(query)
    logger.info(f"Query: {query} and results: {result}")
    return get_metadata(list(result))
