import csv
from pathlib import Path
import time
from typing import Any
from backend.query_evaluator import QueryEvaluator
from numpy import uint32
import pytest



REFERENCES = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000]
COMPARISONS = ["ge"]
PERCENTILES = [0.1, 0.25, 0.5, 0.9]
FAINDERMODES = ['low_memory', 'full_precision', 'full_recall', 'exact']

ADDITIONAL_FILTER_SIZES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]  # in percentage of the total number of hists

ALL = {
        "percentile": 0.1,
        "comparison": "ge",
        "reference": 0.1,
    },

def create_fainder_queries() -> list[dict[str, Any]]:
    fainder_queries = []
    for reference in REFERENCES:
        for comparison in COMPARISONS:
            for percentile in PERCENTILES:
                for fainder_mode in FAINDERMODES:
                    fainder_queries.append({
                        "percentile": percentile,
                        "comparison": comparison,
                        "reference": reference,
                        "fainder_mode": fainder_mode,
                    })
    return fainder_queries

FAINDER_QUERIES = create_fainder_queries()

def log_performance_csv(
    csv_path: Path,
    percentile: float,
    comparison: str,
    reference: int,
    fainder_mode: str,
    additional_filter_size: float,
    execution_time: float,
    filter_size: int,
    results: set[uint32],
):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                percentile,
                comparison,
                reference,
                fainder_mode,
                execution_time,
                additional_filter_size,
                filter_size,
                len(results),
            ]
        )

    
@pytest.mark.parametrize(
    "query",
    FAINDER_QUERIES,
)
def test_fainder_filter_performance(evaluator: QueryEvaluator, query: dict[str, Any]):
    csv_path = Path(pytest.csv_log_path) # type: ignore
    start_time = time.perf_counter()
    result_without_filtering = evaluator.executor.fainder_index.search(query["percentile"], query["comparison"], query["reference"], query["fainder_mode"])
    time_without_filtering = time.perf_counter() - start_time
    num_of_hists = len(evaluator.executor.metadata.hist_to_col)

    log_performance_csv(csv_path, query["percentile"], query["comparison"], query["reference"], query["fainder_mode"], 0, time_without_filtering, 0, result_without_filtering)

    for additional_filter_size in ADDITIONAL_FILTER_SIZES:

        # add to the result without filtering the additional filter
        additional_filter_size = int(additional_filter_size * num_of_hists)
        hist_filter: set[uint32] = set(uint32(i) for i in range(additional_filter_size))
        hist_filter = hist_filter.union(result_without_filtering)

        start_time = time.perf_counter()
        result_with_filtering = evaluator.executor.fainder_index.search(query["percentile"], query["comparison"], query["reference"], query["fainder_mode"], hist_filter)
        time_with_filtering = time.perf_counter() - start_time

        log_performance_csv(csv_path, query["percentile"], query["comparison"], query["reference"], query["fainder_mode"], additional_filter_size, time_with_filtering, additional_filter_size, result_with_filtering)

        assert len(result_with_filtering) == len(result_without_filtering)

