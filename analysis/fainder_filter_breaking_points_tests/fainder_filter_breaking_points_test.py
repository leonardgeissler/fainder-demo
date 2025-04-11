import csv
from pathlib import Path
import time
from typing import Any
from backend.engine.engine import Engine
from backend.engine.executor import doc_to_col_ids, col_to_hist_ids
from backend.config import Metadata
import pytest

from numpy.typing import ArrayLike



REFERENCES = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000]
COMPARISONS = ["le"]
PERCENTILES = [0.1, 0.5, 0.9]
FAINDER_MODES = ["low_memory", "full_precision", "full_recall", "exact"]

KEYWORDS = ["age", "date", "name", "address", "city", "state", "zip", "phone", "email", "web", "germany", "the", "a", "test", "born", "by", "blood", "heart", "lung", "italy", "usa", "bank", "health", "money", "school", "work", "music", "family", "food", "water", "time", "company", "business", "house", "car", "country", "person", "book", "computer", "university", "student", "teacher", "doctor", "hospital", "patient", "science", "technology", "internet", "language", "culture",
           "data", "database", "dataset", "record", "field", "value", "column", "row", "table", "query", "statistic", "metric", "measure", "variable", "attribute", "parameter", "analysis", "analytics", "visualization", "dashboard", "report", "chart", "graph", "api", "json", "xml", "csv", "excel", "sql", "nosql", "index", "key", "schema", "metadata", "timestamp", "integer", "string", "boolean", "float", "array", "object", "null", "dataframe", "dataset", "processing", "pipeline", "etl"]


ALL = (
    {
        "percentile": 0.1,
        "comparison": "ge",
        "reference": 0.1,
    },
)


def create_fainder_queries() -> list[dict[str, Any]]:
    fainder_queries: list[dict[str, Any]] = []
    for reference in REFERENCES:
        for comparison in COMPARISONS:
            for percentile in PERCENTILES:
                for fainder_mode in FAINDER_MODES:
                    fainder_queries.append(
                        {
                            "percentile": percentile,
                            "comparison": comparison,
                            "reference": reference,
                            "query": f"col(pp({percentile}; {comparison}; {reference}))",
                            "fainder_mode": fainder_mode,
                        }
                    )
    return fainder_queries


FAINDER_QUERIES = create_fainder_queries()


def log_performance_csv(
    csv_path: Path,
    query: str,
    fainder_mode: str,
    filter_size_wrong: int,
    filter_size_right: int,
    filter_size: int,
    execution_time: float,
    execution_time_first: float,
    num_results_first: int,
    num_results: int,
):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                fainder_mode,
                filter_size_wrong,
                filter_size_right,
                filter_size,
                execution_time,
                execution_time_first,
                num_results_first,
                num_results,
                query,
            ]
        )

@pytest.mark.parametrize(
    "query",
    FAINDER_QUERIES,
)
def test_breaking_point_fainder(
    engines: tuple[Engine, Engine, Metadata], query: dict[str, Any]
):
    simple_engine, prefiltering_engine, metadata = engines
    csv_path = Path(pytest.csv_log_path)  # type: ignore
    start_time = time.perf_counter()
    result_without_filtering, _ = simple_engine.execute(query["query"], fainder_mode=query["fainder_mode"])
    time_without_filtering = time.perf_counter() - start_time

    log_performance_csv(
        csv_path,
        query["query"],
        query["fainder_mode"],
        0,
        0,
        0,
        time_without_filtering,
        0,
        0,
        len(result_without_filtering),
    )

    keywords = []
    keyword_query = ""
    # find the breaking point for the filter size of the prefiltering engine by adding keywords with an or operator
    for keyword in KEYWORDS:
        keywords.append(keyword)
        keyword_query = " OR ".join(keywords)
        keyword_query = "kw('" + keyword_query + "')"
        start_time = time.perf_counter()
        result_keyword, _ = simple_engine.execute(keyword_query)
        time_keyword = time.perf_counter() - start_time
        result_keyword_hists = col_to_hist_ids(doc_to_col_ids(set(result_keyword), metadata.doc_to_cols), metadata.col_to_hist)
        
        query_string = f"{keyword_query} AND {query['query']}"
        start_time = time.perf_counter()
        result_keyword_filter, _ = prefiltering_engine.execute(query_string, fainder_mode=query["fainder_mode"])
        time_with_keyword_filter = time.perf_counter() - start_time
        result_keyword_filter_hists = col_to_hist_ids(doc_to_col_ids(set(result_keyword_filter), metadata.doc_to_cols), metadata.col_to_hist)

        if time_without_filtering < time_with_keyword_filter:
            break

        # use result_keyword and result_without_filtering to calculate the filter size right and wrong
        filter_size_right = len(result_keyword_filter_hists)
        filter_size_wrong = len(result_keyword_hists) - filter_size_right
        filter_size = len(result_keyword_hists)


        log_performance_csv(
            csv_path,
            query_string,
            query["fainder_mode"],
            filter_size_wrong,
            filter_size_right,
            filter_size,
            time_with_keyword_filter,
            time_keyword,
            len(result_keyword),
            len(result_keyword_filter),
        )



