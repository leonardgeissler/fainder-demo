from typing import TypedDict

from backend.config import FainderMode


# sizes at which point to stop prefiltering per Fainder mode
class FilteringStopPointsConfig(TypedDict):
    doc_ids: int
    col_ids: int
    hist_ids: int


FilteringStopPointsMap = dict[FainderMode, FilteringStopPointsConfig]

filtering_stop_points: dict[FainderMode, FilteringStopPointsConfig] = {
    FainderMode.LOW_MEMORY: {
        "doc_ids": 50000,
        "col_ids": 500000,
        "hist_ids": 500000,
    },
    FainderMode.FULL_PRECISION: {
        "doc_ids": 50000,
        "col_ids": 500000,
        "hist_ids": 500000,
    },
    FainderMode.FULL_RECALL: {
        "doc_ids": 50000,
        "col_ids": 500000,
        "hist_ids": 500000,
    },
    FainderMode.EXACT: {
        "doc_ids": 100000,
        "col_ids": 1000000,
        "hist_ids": 1000000,
    },
}
