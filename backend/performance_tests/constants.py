from typing import Final

# Keyword related constants
DEFAULT_KEYWORDS: Final[list[str]] = [
    "test",
    "born",
    "by",
    "blood",
    "heart",
    "lung",
    "test",
    "germany",
    "italy",
    "usa",
    "bank",
]  # TODO: add more with different number of results

DEFAULT_COL_NAMES: Final[list[str]] = [
    "age",
    "date",
    "country",
]

DEFAULT_KS: Final[list[int]]= [0, 1, 3, 5]


# Percentile related constants
DEFAULT_THRESHOLDS: Final[list[int]] = [10, 1000, 10000, 1000000]
DEFAULT_PERCENTILES: Final[list[float]] = [0.5, 0.9]
DEFAULT_OPERATORS: Final[list[str]] = ["ge", "le"]

# Logical operators
LOGICAL_OPERATORS: Final[list[str]] = ["AND"]

# Maximum number of terms to combine
MIN_NUM_TERMS_QUERY: Final[int] = 2
MAX_NUM_TERMS_QUERY: Final[int] = 4
MAX_NUM_QUERY_PER_NUM_TERMS: Final[int] = 50

FAINDER_MODES: Final[list[str]] = ["low_memory", "full_precision", "full_recall", "exact"]
