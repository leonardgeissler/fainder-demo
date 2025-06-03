import csv
from pathlib import Path
import time
from typing import Any
from backend.engine.engine import Engine
from backend.engine.conversion import col_to_doc_ids, col_to_hist_ids, doc_to_col_ids
from backend.config import Metadata, Settings
from backend.indices.percentile_op import FainderIndex
import pytest


REFERENCES = [1, 100000, 10000000]
COMPARISONS = ["le", "ge"]
PERCENTILES = [0.1, 0.5, 0.9]
FAINDER_MODES = ["full_recall", "exact"]

KEYWORDS =['zeppelin', 'vampire', 'zebra', 'zombie', 'yacht', 'dinosaur', 'ninja', 'wizard', 'pirate', 'quantum', 'volcano', 'unicorn', 'dragon', 'spaceship', 'lung', 'born', 'italy', 'germany', 'q*', 'z*', 'blood', 'zip', 'money', 'phone', 'w*', 'v*', 'f*', 'h*', 'j*', 'family', 'l*', 'usa', 'email', 'water', 'school', 'y*', 'k*', 'x*', 'heart', 'n*', 'music', 'u*', 'p*', 'address', 'c*', 'd*', 'o*', 'r*', 'web', 'bank', 'm*', 'food', 'b*', 'g*', 'age', 'city', 't*', 'work', 'test', 'e*', 'state', 'i*', 'date', 'company', 'name', 'health', 's*', 'time', 'by', 'a', 'a*', 'the']


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
    filter_size_wrong_doc: int = 0,
    filter_size_right_doc: int = 0,
    filter_size_doc: int = 0,
    num_workers: int = 1,
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
                filter_size_wrong_doc,
                filter_size_right_doc,
                filter_size_doc,
                execution_time,
                execution_time_first,
                num_results_first,
                num_results,
                query,
                num_workers,
            ]
        )


@pytest.mark.parametrize(
    "query",
    FAINDER_QUERIES,
)
def test_breaking_point_fainder(
    engines: tuple[Engine, FainderIndex, Metadata, Settings], query: dict[str, Any]
):
    simple_engine, fainder_index, metadata, settings = engines
    csv_path = Path(pytest.csv_log_path)  # type: ignore
    start_time = time.perf_counter()
    result_without_filtering, _ = simple_engine.execute(
        query["query"], fainder_mode=query["fainder_mode"]
    )
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
        0,
        0,
        0,
        num_workers=settings.fainder_num_workers,
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
        import numpy as np

        result_keyword_hists = col_to_hist_ids(
            doc_to_col_ids(
                np.array(list(result_keyword), dtype=np.uint32), metadata.doc_to_cols
            ),
            metadata.num_hists,
        )

        query_string = f"{keyword_query} AND {query['query']}"
        start_time = time.perf_counter()
        result_keyword_filter_hists = fainder_index.search(
            query["percentile"],
            query["comparison"],
            query["reference"],
            query["fainder_mode"],
            result_keyword_hists,
        )
        result_keyword_filter = col_to_doc_ids(
            result_keyword_filter_hists, metadata.col_to_doc
        )
        time_with_keyword_filter = time.perf_counter() - start_time
        result_keyword_filter_hists = col_to_hist_ids(
            doc_to_col_ids(
                np.array(list(result_keyword_filter), dtype=np.uint32),
                metadata.doc_to_cols,
            ),
            metadata.num_hists,
        )

        # use result_keyword and result_without_filtering to calculate the filter size right and wrong
        filter_size_right = len(result_keyword_filter_hists)
        filter_size_wrong = len(result_keyword_hists) - filter_size_right
        filter_size = len(result_keyword_hists)

        # calculate the filter size on document level
        filter_size_right_doc = len(result_keyword_filter)
        filter_size_wrong_doc = len(result_keyword) - filter_size_right_doc
        filter_size_doc = len(result_keyword)

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
            filter_size_wrong_doc,
            filter_size_right_doc,
            filter_size_doc,
            num_workers=settings.fainder_num_workers,
        )

        if time_without_filtering < time_with_keyword_filter:
            return
