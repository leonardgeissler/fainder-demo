import csv
from pathlib import Path
import time
from backend.query_evaluator import QueryEvaluator
from numpy import uint32
import pytest


NAMES = ["age", "date"]
K = 2

ADDITIONAL_FILTER_SIZES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

def log_performance_csv(
    csv_path: Path,
    name: str,
    k: int,
    execution_time: float,
    additional_filter_size: float,
    filter_size: int,
    results: set[uint32],
):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                name,
                k,
                execution_time,
                additional_filter_size,
                filter_size,
                len(results),
            ]
        )

@pytest.mark.parametrize("name", NAMES)
def test_hnsw_filter_performance(evaluator: QueryEvaluator, name: str):
    csv_path = Path(pytest.csv_log_path_hnsw) # type: ignore
    start_time = time.perf_counter()
    result_without_filtering = evaluator.executor.hnsw_index.search(name, K, None)
    time_without_filtering = time.perf_counter() - start_time

    log_performance_csv(csv_path, name, K, time_without_filtering, 0, 0, result_without_filtering)

    num_col_ids = len(evaluator.executor.metadata.col_to_hist.values())

    for additional_filter_size in ADDITIONAL_FILTER_SIZES:
        filter_size = int(num_col_ids * additional_filter_size)
        col_ids: set[uint32] = {uint32(i) for i in range(filter_size)}
        col_ids = col_ids.union({uint32(x) for x in result_without_filtering})
        start_time = time.perf_counter()
        results = evaluator.executor.hnsw_index.search(name, K, col_ids)
        execution_time = time.perf_counter() - start_time
        log_performance_csv(csv_path, name, K, execution_time, additional_filter_size, filter_size, results)





