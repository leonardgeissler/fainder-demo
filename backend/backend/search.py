import os
from typing import Any

from fainder.execution.runner import run
from loguru import logger

from backend.config import INDEX, LIST_OF_DOCS
from backend.grammar import evaluate_new_query, evaluate_query
from backend.lucene_connector import LuceneConnector
from backend.utils import (
    get_hists_for_doc_ids,
    get_metadata,
    number_of_matching_histograms_to_doc_number,
    parse_precentile_query,
)


def search(query: str) -> list[dict[str, Any]]:
    result = evaluate_new_query(query)
    logger.info(f"Query '{query}' returned: {result}")
    return get_metadata(list(result))


def search_basic(percentile: str, keywords: str) -> list[dict[str, Any]]:
    """Deprecated function, maintained for benchmarking purposes."""
    lucene_connector = LuceneConnector(
        os.getenv("LUCENE_HOST", "127.0.0.1"), os.getenv("LUCENE_PORT", "8001")
    )
    results_keywords, _ = lucene_connector.evaluate_query(keywords)

    if len(results_keywords) == 0:
        return []

    filter_hist = get_hists_for_doc_ids(results_keywords)
    matching_docs = list(evaluate_query(percentile, filter_hist))
    return get_metadata(matching_docs)


def search_combined(percentile: str, keywords: str) -> list[dict[str, Any]]:
    """Deprecated function, maintained for benchmarking purposes."""
    lucene_connector = LuceneConnector(
        os.getenv("LUCENE_HOST", "127.0.0.1"), os.getenv("LUCENE_PORT", "8001")
    )
    results_keywords, _ = lucene_connector.evaluate_query(keywords)
    matching_docs = list(evaluate_query(percentile))
    combined_results = [i for i in results_keywords if i in matching_docs]
    return get_metadata(combined_results)


def search_single(percentile: str, keywords: str) -> list[dict[str, Any]]:
    """Deprecated function, maintained for benchmarking purposes."""
    lucene_connector = LuceneConnector(
        os.getenv("LUCENE_HOST", "127.0.0.1"), os.getenv("LUCENE_PORT", "8001")
    )
    results_keywords, _ = lucene_connector.evaluate_query(keywords)
    query = parse_precentile_query(percentile)
    result = run(INDEX, [query], "index")
    matching_histograms = result[0]
    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])
    combined_results = [i for i in results_keywords if i in matching_docs]
    return get_metadata(combined_results)


def query_bit_array(percentile: str) -> list[int]:
    """Deprecated function, maintained for benchmarking purposes."""
    query = parse_precentile_query(percentile)
    result = run(INDEX, [query], "index")
    matching_histograms = result[0]
    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])
    return [1 if i in matching_docs else 0 for i in range(len(LIST_OF_DOCS))]
