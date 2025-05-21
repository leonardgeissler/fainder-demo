import csv
from pathlib import Path
import time
from backend.config import Metadata
from backend.indices.name_op import HnswIndex
from numpy import uint32
import pytest


NAMES = ["age", "date"]
K = 2

FILTER_SIZES_RIGHT = [
    100, 1000, 10000, 100000, 1000000, 10000000
] 

FILTER_SIZES_WRONG = [
    0, 100, 1000, 10000, 100000, 1000000, 10000000
]



def log_performance_csv(
    csv_path: Path,
    name: str,
    k: int,
    execution_time: float,
    FILTER_SIZES_RIGHT: int,
    FILTER_SIZES_WRONG: int,
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
                FILTER_SIZES_RIGHT,
                FILTER_SIZES_WRONG,
                filter_size,
                len(results),
            ]
        )


@pytest.mark.parametrize("name", NAMES)
def test_hnsw_filter_performance(hnsw: tuple[HnswIndex, Metadata], name: str):
    hnsw_index, metadata = hnsw
    csv_path = Path(pytest.csv_log_path_hnsw)  # type: ignore
    start_time = time.perf_counter()
    result_without_filtering = hnsw_index.search(name, K, None)
    time_without_filtering = time.perf_counter() - start_time

    log_performance_csv(
        csv_path, name, K, time_without_filtering, 0, 0, 0, result_without_filtering
    )

    num_col_ids = len(metadata.col_to_hist.values())

    for filter_size_right in FILTER_SIZES_RIGHT:
        for filter_size_wrong in FILTER_SIZES_WRONG:
            
            filter_size_right = min(filter_size_right, len(result_without_filtering))
            filter_size_wrong = min(filter_size_wrong, num_col_ids - filter_size_right)

            # create a filter of the desired size by selecting indices out of the results without filtering (not random)
            col_ids = list(result_without_filtering)[:filter_size_right]

            # add some indices that are not in the results without filtering 
            filter_wrong = list({uint32(x) for x in range(num_col_ids)} - {x for x in result_without_filtering})
            filter_wrong = filter_wrong[:filter_size_wrong]
            col_ids.extend(filter_wrong)

            set_col_ids = set(col_ids)

            start_time = time.perf_counter()
            results = hnsw_index.search(name, K, set_col_ids)
            execution_time = time.perf_counter() - start_time
            log_performance_csv(
                csv_path,
                name,
                K,
                execution_time,
                filter_size_right,
                filter_size_wrong,
                filter_size_right + filter_size_wrong,
                results,
            )
