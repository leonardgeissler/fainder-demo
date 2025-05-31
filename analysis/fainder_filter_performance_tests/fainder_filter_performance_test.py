import csv
from pathlib import Path
import re
import time
from typing import Any, Literal
from backend.indices.percentile_op import FainderIndex
from backend.config import Metadata, FainderMode
from numpy import uint32
import pytest
import numpy as np

from numpy.typing import ArrayLike, NDArray


REFERENCES = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000]
COMPARISONS = ["le"]
PERCENTILES = [0.1, 0.25, 0.5, 0.9]
FAINDER_MODES = [FainderMode.FULL_RECALL, FainderMode.EXACT]

FILTER_SIZES_RIGHT = [
    100, 1000, 10000, 100000, 1000000, 10000000
] 

FILTER_SIZES_WRONG = [
    0, 100, 1000, 10000, 100000, 1000000, 10000000
]


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
                            "fainder_mode": fainder_mode,

                        }
                    )
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
    results: NDArray[np.uint32],
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
                results.size
            ]
        )


def run(
    fainder: FainderIndex,
    query: dict[str, Any],
    fainder_mode: FainderMode,
    hist_filter: ArrayLike | None = None,
) -> NDArray[np.uint32]:

    if hist_filter is not None:
        hist_filter = np.array(hist_filter)
    return fainder.search(
        query["percentile"],
        query["comparison"],
        query["reference"],
        fainder_mode,
        hist_filter
    )



@pytest.mark.parametrize(
    "query",
    FAINDER_QUERIES,
)
def test_fainder_filter_performance(
    fainder: FainderIndex, metadata: Metadata, query: dict[str, Any]
):
    csv_path = Path(pytest.csv_log_path)  # type: ignore
    start_time = time.perf_counter()
    result_without_filtering, _ = run(
        fainder,
        query,
        query["fainder_mode"],
    )
    time_without_filtering = time.perf_counter() - start_time
    num_of_hists = metadata.num_hists

    log_performance_csv(
        csv_path,
        query["percentile"],
        query["comparison"],
        query["reference"],
        query["fainder_mode"],
        0,
        time_without_filtering,
        0,
        result_without_filtering,
    )

    for filter_size_right in FILTER_SIZES_RIGHT:
        for filter_size_wrong in FILTER_SIZES_WRONG:
            
            filter_size_right = min(filter_size_right, len(result_without_filtering))
            filter_size_wrong = min(filter_size_wrong, num_of_hists - filter_size_right)

            # create a filter of the desired size by selecting indices out of the results without filtering (not random)
            hist_filter = list(result_without_filtering)[:filter_size_right]

            # add some indices that are not in the results without filtering 
            filter_wrong = list({uint32(x) for x in range(num_of_hists)} - {x for x in result_without_filtering})
            filter_wrong = filter_wrong[:filter_size_wrong]
            hist_filter.extend(filter_wrong)

            np_filter = np.array(list(hist_filter))

            start_time = time.perf_counter()
            result_with_filtering, _ = run(
                fainder,
                query,
                query["fainder_mode"],
                np_filter,
            )
            time_with_filtering = time.perf_counter() - start_time

            log_performance_csv(
                csv_path,
                query["percentile"],
                query["comparison"],
                query["reference"],
                query["fainder_mode"],
                filter_size_right,
                time_with_filtering,
                len(np_filter),
                result_with_filtering,
            )

            # check result with filtering is a subset of result without filtering
            #assert len(result_with_filtering) == filter_size_right
