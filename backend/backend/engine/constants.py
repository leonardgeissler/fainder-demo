from typing import TypedDict

from backend.config import FainderMode


class FilteringStopPointsConfig(TypedDict):
    """sizes at which point to stop prefiltering per Fainder mode"""

    num_doc_ids: int
    num_col_ids: int
    num_hist_ids: int


FILTERING_STOP_POINTS: dict[FainderMode, FilteringStopPointsConfig] = {
    FainderMode.LOW_MEMORY: {
        "num_doc_ids": 1000,
        "num_col_ids": 10000,
        "num_hist_ids": 10000,
    },
    FainderMode.FULL_PRECISION: {
        "num_doc_ids": 1000,
        "num_col_ids": 10000,
        "num_hist_ids": 10000,
    },
    FainderMode.FULL_RECALL: {
        "num_doc_ids": 1000,
        "num_col_ids": 10000,
        "num_hist_ids": 10000,
    },
    FainderMode.EXACT: {
        "num_doc_ids": 20000,
        "num_col_ids": 300000,
        "num_hist_ids": 300000,
    },
}
