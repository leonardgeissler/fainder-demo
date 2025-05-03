from typing import Final

# Keyword related constants
DEFAULT_KEYWORDS: Final[list[str]] = [
    "test",
    "born",
    "by",
    
]

DEFAULT_COL_NAMES: Final[list[str]] = [
    "age",
    "date",
]

DEFAULT_KS: Final[list[int]] = [0, 2, 5]


# Percentile related constants
DEFAULT_THRESHOLDS: Final[list[int]] = [100, 1000000]
DEFAULT_PERCENTILES: Final[list[float]] = [0.5, 0.9]
DEFAULT_OPERATORS: Final[list[str]] = ["ge", "le"]

# Logical operators
LOGICAL_OPERATORS: Final[list[str]] = ["AND"]

# Maximum number of terms to combine
MIN_NUM_TERMS_QUERY: Final[int] = 2
MAX_NUM_TERMS_QUERY: Final[int] = 4
MAX_NUM_QUERY_PER_NUM_TERMS: Final[int] = 20

FAINDER_MODES: Final[list[str]] = ["low_memory", "full_precision", "full_recall", "exact"]

MAX_NUM_MIXED_TERMS_WITH_FIXED_STRUCTURE: Final[int] = 30
MAX_NUM_MIXED_TERMS_EXTENTED_WITH_FIXED_STRUCTURE: Final[int] = 20

MAX_NUM_OF_NESTED_TERMS_PER_LEVEL: Final[int] = 10
MIN_NESTED_LEVEL: Final[int] = 2
MAX_NESTED_LEVEL: Final[int] = 4

ENABLED_TESTS: Final[list[str]] = [
    "base_keyword_queries",
    "base_percentile_queries",
    "base_percentile_queries",
    "mixed_combinations_with_fixed_structure",
    "mixed_combinations_with_fixed_structure_extented",
    "early_exit",
    "multiple_percentile_combinations",
    "multiple_percentile_combinations_with_kw"
]
