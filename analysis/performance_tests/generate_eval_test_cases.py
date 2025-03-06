import json
import random
from itertools import combinations
from pathlib import Path
from typing import Any

from performance_tests.constants import (
    DEFAULT_HIGH_PERCENTILE,
    DEFAULT_KEYWORDS,
    DEFAULT_LARGE_THRESHOLDS,
    DEFAULT_OPERATORS,
    DEFAULT_SMALL_PERCENTILE,
    DEFAULT_SMALL_THRESHOLDS,
    LOGICAL_OPERATORS,
    MAX_NUM_QUERY_PER_NUM_TERMS,
    MAX_NUM_TERMS_QUERY,
    MIN_NUM_TERMS_QUERY,
)


def generate_base_keyword_queries() -> dict[str, dict[str, Any]]:
    return {
        "double_wildcard_search": {"query": "kw(*a*)"},
        "wildcard_search": {"query": "kw(a*)"},
    }


def generate_simple_keyword_queries(
    keywords: list[str] = DEFAULT_KEYWORDS,
    prefix: str = "simple_keyword",
    field_name: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Generate simple keyword queries.

    Args:
        keywords: List of keywords to search for. If None, uses default list
        prefix: Prefix for the query names
        field_name: Optional field name for field-specific searches
    """

    field_prefix = f"{field_name}:" if field_name else ""
    return {
        f"{prefix}_{i + 1}": {
            "query": f"kw({field_prefix}{word})",
            "keyword_id": f"{field_prefix}{word}",
        }
        for i, word in enumerate(keywords)
    }


def generate_complex_keyword_queries(base_word: str) -> dict[str, dict[str, Any]]:
    return {
        "simple_keyword": {"query": f"kw({base_word})"},
        "not_keyword": {"query": f"NOT kw({base_word})"},
        "wildcard_search": {"query": f"kw({base_word[0]}u?{base_word[-1]})"},
        "double_wildcard_searches": {"query": f"kw(?{base_word[1]}?{base_word[-1]})"},
        "field_specific_keyword": {"query": f'kw(alternateName:"{base_word.capitalize()}")'},
    }


def generate_percentile_terms(
    small_percentile: float = DEFAULT_SMALL_PERCENTILE,
    high_percentile: float = DEFAULT_HIGH_PERCENTILE,
    small_thresholds: list[int] = DEFAULT_SMALL_THRESHOLDS,
    large_thresholds: list[int] = DEFAULT_LARGE_THRESHOLDS,
    operators: dict[str, list[str]] = DEFAULT_OPERATORS,
) -> list[str]:
    """
    Generate percentile terms for testing.

    Args:
        small_percentile: Percentile value for small thresholds
        high_percentile: Percentile value for large thresholds
        small_thresholds: List of small threshold values. If None, uses defaults
        large_thresholds: List of large threshold values. If None, uses defaults
        operators: Dictionary of operators to use for each threshold type
    """
    terms: list[str] = []

    # Generate small threshold terms
    for op in operators.get("small", ["le"]):
        for threshold in small_thresholds:
            terms.append(f"pp({small_percentile};{op};{threshold})")

    # Generate large threshold terms
    for op in operators.get("large", ["ge"]):
        for threshold in large_thresholds:
            terms.append(f"pp({high_percentile};{op};{threshold})")

    return terms


def generate_percentile_queries(
    small_percentile: float = DEFAULT_SMALL_PERCENTILE,
    high_percentile: float = DEFAULT_HIGH_PERCENTILE,
    small_thresholds: list[int] = DEFAULT_SMALL_THRESHOLDS,
    large_thresholds: list[int] = DEFAULT_LARGE_THRESHOLDS,
    operators: dict[str, list[str]] = DEFAULT_OPERATORS,
) -> dict[str, dict[str, Any]]:
    """
    Generate percentile queries for testing.

    Args:
        small_percentile: Percentile value for small thresholds
        high_percentile: Percentile value for large thresholds
        small_thresholds: List of small threshold values. If None, uses defaults
        large_thresholds: List of large threshold values. If None, uses defaults
        operators: Dictionary of operators to use for each threshold type
    """

    queries: dict[str, dict[str, Any]] = {}

    # Generate small threshold queries
    for op in operators.get("small", ["le"]):
        for i, threshold in enumerate(small_thresholds, 1):
            queries[f"small_percentile_{op}_{i}"] = {
                "query": f"col(pp({small_percentile};{op};{threshold}))",
                "percentile_id": f"pp({small_percentile};{op};{threshold})",
            }

    # Generate large threshold queries
    for op in operators.get("large", ["ge"]):
        for i, threshold in enumerate(large_thresholds, 1):
            queries[f"high_percentile_{op}_{i}"] = {
                "query": f"col(pp({high_percentile};{op};{threshold}))",
                "percentile_id": f"pp({high_percentile};{op};{threshold})",
            }

    return queries


def wrap_term(term: str) -> str:
    """Wrap a term with appropriate function based on its type."""
    if "col(" in term:
        return term  # Already wrapped with col()
    if any(x in term for x in ["pp(", "name("]):
        return f"col({term})"
    return f"kw({term})" if not any(x in term for x in ["kw("]) else term


def keyword_combinations(
    keywords: list[str],
    operators: list[str] = LOGICAL_OPERATORS,
    min_terms: int = MIN_NUM_TERMS_QUERY,
    max_terms: int = MAX_NUM_TERMS_QUERY,
    num_query_per_num_terms: int = MAX_NUM_QUERY_PER_NUM_TERMS,
    internal_combinations: bool = False,
) -> dict[str, dict[str, Any]]:
    """
    Generate keyword combinations for testing.

    Args:
        keywords: List of keywords to combine
        operators: List of logical operators to use
        max_terms: Maximum number of terms to combine
    """
    queries: dict[str, dict[str, Any]] = {}

    # Generate all possible combinations of keywords
    helper: list[int] = []
    for i in range(0, len(keywords)):
        helper.append(i)
    query_counter = 1
    for operator in operators:
        # Generate combinations for each length from 2 to max_terms
        for num_terms in range(min_terms, max_terms + 1):
            i = 0
            for combination in combinations(helper, num_terms):
                i += 1
                if len(combination) == 0:
                    continue
                combination_keywords: list[str] = []
                for term in combination:
                    if internal_combinations:
                        combination_keywords.append(keywords[term])
                    else:
                        combination_keywords.append(wrap_term(keywords[term]))
                query = f" {operator} ".join(combination_keywords)
                if internal_combinations:
                    query = wrap_term(query)
                queries[f"keyword_combination_{operator}_{query_counter}"] = {
                    "query": query,
                    "ids": [{"keyword_id": keywords[term]} for term in combination],
                }
                query_counter += 1
                if i > num_query_per_num_terms:
                    break
    return queries


def percentile_term_combinations(
    terms: list[str],
    operators: list[str] = LOGICAL_OPERATORS,
    min_terms: int = MIN_NUM_TERMS_QUERY,
    max_terms: int = MAX_NUM_TERMS_QUERY,
    num_query_per_num_terms: int = MAX_NUM_QUERY_PER_NUM_TERMS,
) -> dict[str, dict[str, Any]]:
    """
    Generate percentile term combinations for testing.

    Args:
        terms: List of terms to combine
        operators: List of logical operators to use
        max_terms: Maximum number of terms to combine
    """
    queries: dict[str, dict[str, Any]] = {}

    # Generate all possible combinations of terms
    helper: list[int] = []
    for i in range(0, len(terms)):
        helper.append(i)

    query_counter = 1
    for operator in operators:
        # Generate combinations for each length from 2 to max_terms
        for num_terms in range(min_terms, max_terms + 1):
            i = 0
            for combination in combinations(helper, num_terms):
                if len(combination) == 0:
                    continue
                combination_terms: list[str] = []
                for term in combination:
                    combination_terms.append(terms[term])
                query = f" {operator} ".join(combination_terms)
                query = wrap_term(query)
                queries[f"percentile_combination_{operator}_{query_counter}"] = {
                    "query": query,
                    "ids": [{"percentile_id": terms[term]} for term in combination],
                }
                query_counter += 1
                i += 1
                if i > num_query_per_num_terms:
                    break
    return queries


def mixed_term_combinations(
    keywords: list[str],
    terms: list[str],
    operators: list[str] = LOGICAL_OPERATORS,
    min_terms: int = MIN_NUM_TERMS_QUERY,
    max_terms: int = MAX_NUM_TERMS_QUERY,
    num_query_per_num_terms: int = MAX_NUM_QUERY_PER_NUM_TERMS,
) -> dict[str, dict[str, Any]]:
    """
    Generate mixed term combinations for testing.
    With alternating keyword and percentile terms.
    """
    queries: dict[str, dict[str, Any]] = {}

    # Generate all possible combinations of terms
    helper: list[tuple[str, int]] = []
    for i in range(0, len(terms)):
        helper.append(("percentile", i))
    for i in range(0, len(keywords)):
        helper.append(("keyword", i))

    query_counter = 1
    for operator in operators:
        # Generate combinations for each length from 2 to max_terms
        for num_terms in range(min_terms, max_terms + 1):
            i = 0
            for combination in combinations(helper, num_terms):
                if len(combination) == 0:
                    continue
                combination_list = list(combination)
                random.shuffle(combination_list)

                combination_terms: list[str] = []
                for term, index in combination_list:
                    if term == "keyword":
                        combination_terms.append(wrap_term(keywords[index]))
                    else:
                        combination_terms.append(wrap_term(terms[index]))

                query = f" {operator} ".join(combination_terms)
                queries[f"mixed_combination_{operator}_{query_counter}"] = {
                    "query": query,
                    "ids": [
                        {"keyword_id": keywords[index]}
                        if term == "keyword"
                        else {"percentile_id": terms[index]}
                        for term, index in combination_list
                    ],
                }
                query_counter += 1
                i += 1
                if i > num_query_per_num_terms:
                    break
    return queries


def generate_all_test_cases() -> dict[str, Any]:
    keywordsqueries = generate_simple_keyword_queries(DEFAULT_KEYWORDS)
    percentile_terms = generate_percentile_terms()
    percentilequeries = generate_percentile_queries()

    keyword_combinations_queries = keyword_combinations(DEFAULT_KEYWORDS)
    percentile_combinations_queries = percentile_term_combinations(percentile_terms)

    mixed_combination_queries = mixed_term_combinations(DEFAULT_KEYWORDS, percentile_terms)

    return {
        "base_keyword_queries": {"queries": keywordsqueries},
        "base_percentile_queries": {
            "queries": percentilequeries,
        },
        "keyword_combinations": {"queries": keyword_combinations_queries},
        "percentile_combinations": {"queries": percentile_combinations_queries},
        "mixed_combinations": {
            "queries": mixed_combination_queries,
        },
    }


def save_test_cases(output_path: Path) -> None:
    """Generate test cases and save them to a JSON file."""
    test_cases = generate_all_test_cases()
    with output_path.open("w") as f:
        json.dump(test_cases, f, indent=2)


if __name__ == "__main__":
    output_dir = Path("test_cases")
    output_dir.mkdir(exist_ok=True)
    save_test_cases(output_dir / "performance_test_cases.json")
