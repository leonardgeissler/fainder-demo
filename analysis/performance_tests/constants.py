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


# Percentile related constants
DEFAULT_SMALL_THRESHOLDS: Final[list[int]] = [2000, 1000, 500, 200]
DEFAULT_LARGE_THRESHOLDS: Final[list[int]] = [1000000, 2000000, 3000000, 4000000]
DEFAULT_SMALL_PERCENTILE: Final[float] = 0.5
DEFAULT_HIGH_PERCENTILE: Final[float] = 0.9
DEFAULT_OPERATORS: Final[dict[str, list[str]]] = {"lesser": ["le"], "bigger": ["ge"]}

# Logical operators
LOGICAL_OPERATORS: Final[list[str]] = ["AND"]

# Maximum number of terms to combine
MIN_NUM_TERMS_QUERY: Final[int] = 2
MAX_NUM_TERMS_QUERY: Final[int] = 6
MAX_NUM_QUERY_PER_NUM_TERMS: Final[int] = 100
