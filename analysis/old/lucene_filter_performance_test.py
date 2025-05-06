import csv
from pathlib import Path
import time
from typing import Sequence
from backend.lucene_connector import LuceneConnector
from backend.engine import Engine
import pytest

KEYWORDS = ["*a*", "test", "germany", "lung", "data", "a", "the"]

ADDITIONAL_FILTER_SIZES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]


def log_performance_csv(
    csv_path: Path,
    keyword: str,
    execution_time: float,
    additional_filter_size: float,
    filter_size: int,
    results: Sequence[int],
):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                keyword,
                execution_time,
                additional_filter_size,
                filter_size,
                len(results),
            ]
        )


@pytest.mark.parametrize("keyword", KEYWORDS)
def test_lucene_filter_performance(evaluator: Engine, keyword: str):
    csv_path = Path(pytest.csv_log_path_lucene)  # type: ignore
    lucene_connector = LuceneConnector("127.0.0.1", "8001")
    start_time = time.perf_counter()
    result_without_filtering, _, _ = lucene_connector.evaluate_query(keyword)
    time_without_filtering = time.perf_counter() - start_time
    log_performance_csv(
        csv_path, keyword, time_without_filtering, 0, 0, result_without_filtering
    )

    num_doc_ids = len(evaluator.executor.metadata.doc_to_cols.values())

    for additional_filter_size in ADDITIONAL_FILTER_SIZES:
        filter_size = int(num_doc_ids * additional_filter_size)
        doc_ids = set(range(filter_size))
        doc_ids = doc_ids.union(result_without_filtering)
        start_time = time.perf_counter()
        results, _, _ = lucene_connector.evaluate_query(keyword, doc_ids)
        execution_time = time.perf_counter() - start_time
        log_performance_csv(
            csv_path,
            keyword,
            execution_time,
            additional_filter_size,
            filter_size,
            results,
        )
