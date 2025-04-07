import json
from itertools import combinations
from pathlib import Path
from typing import Any

from .constants import (
    DEFAULT_COL_NAMES,
    DEFAULT_KEYWORDS,
    DEFAULT_KS,
    DEFAULT_OPERATORS,
    DEFAULT_PERCENTILES,
    DEFAULT_THRESHOLDS,
    LOGICAL_OPERATORS,
    MAX_NUM_QUERY_PER_NUM_TERMS,
    MAX_NUM_TERMS_QUERY,
    MIN_NUM_TERMS_QUERY,
)


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
            "query": f"kw('{field_prefix}{word}')",
            "keyword_id": f"'{field_prefix}{word}'",
        }
        for i, word in enumerate(keywords)
    }


def generate_percentile_terms(
    percentile_values: list[float] = DEFAULT_PERCENTILES,
    thresholds: list[int] = DEFAULT_THRESHOLDS,
    operators: list[str] = DEFAULT_OPERATORS,
) -> list[str]:
    """
    Generate percentile terms for testing.

    Args:
        percentile_values: List of percentile values
        thresholds: List of threshold values
        operators: List of operators to use
    """
    terms: list[str] = []

    for op in operators:
        for threshold in thresholds:
            terms.extend(
                [f"pp({percentile};{op};{threshold})" for percentile in percentile_values]
            )

    return terms


def generate_percentile_queries(
    percentile_values: list[float] = DEFAULT_PERCENTILES,
    thresholds: list[int] = DEFAULT_THRESHOLDS,
    operators: list[str] = DEFAULT_OPERATORS,
) -> dict[str, dict[str, Any]]:
    """
    Generate percentile queries for testing.

    Args:
        percentile_values: List of percentile values
        thresholds: List of threshold values
        operators: List of operators to use
    """

    queries: dict[str, dict[str, Any]] = {}

    for op in operators:
        for i, threshold in enumerate(thresholds):
            for h, percentile in enumerate(percentile_values):
                queries[f"percentile_{op}_{i}_{h}"] = {
                    "query": f"col(pp({percentile};{op};{threshold}))",
                    "percentile_id": f"pp({percentile};{op};{threshold})",
                }

    return queries


def wrap_term(term: str) -> str:
    """Wrap a term with appropriate function based on its type."""
    if "col(" in term:
        return term  # Already wrapped with col()
    if any(x in term for x in ["pp(", "name("]):
        return f"col({term})"
    return f"kw('{term}')" if not any(x in term for x in ["kw("]) else term


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
    helper = list(range(len(keywords)))

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
    helper = list(range(len(terms)))

    query_counter = 1
    for operator in operators:
        # Generate combinations for each length from 2 to max_terms
        for num_terms in range(min_terms, max_terms + 1):
            i = 0
            for combination in combinations(helper, num_terms):
                if len(combination) == 0:
                    continue
                combination_terms = [terms[term] for term in combination]
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


def mixed_term_combinations_with_fixed_structure(
    keywords: list[str],
    terms: list[str],
    column_names: list[str] = DEFAULT_COL_NAMES,
    ks: list[int] = DEFAULT_KS,
    operators: list[str] = LOGICAL_OPERATORS,
) -> dict[str, dict[str, Any]]:
    # query structure: kw('test') AND col(name('age',1) AND pp(0.5;le;2000))

    queries: dict[str, dict[str, Any]] = {}

    query_counter = 1
    for operator in operators:
        for keyword in keywords:
            for percentile in terms:
                for column_name in column_names:
                    for k in ks:
                        query = (
                            f"kw('{keyword}') {operator} "
                            f"col(name('{column_name}';{k}) {operator} {percentile})"
                        )
                        queries[f"mixed_combination_{operator}_{query_counter}"] = {
                            "query": query,
                            "ids": [
                                {"keyword_id": keyword},
                                {"percentile_id": percentile},
                                {"column_id": (column_name, k)},
                            ],
                        }
                        query_counter += 1

    return queries


def generate_all_test_cases() -> dict[str, Any]:
    keywordsqueries = generate_simple_keyword_queries(DEFAULT_KEYWORDS)
    percentile_terms = generate_percentile_terms()
    percentilequeries = generate_percentile_queries()

    # keyword_combinations_queries = keyword_combinations(DEFAULT_KEYWORDS)
    percentile_combinations_queries = percentile_term_combinations(percentile_terms)

    mixed_term_combinations_with_fixed_structure_queries = (
        mixed_term_combinations_with_fixed_structure(DEFAULT_KEYWORDS, percentile_terms)
    )

    return {
        "base_keyword_queries": {"queries": keywordsqueries},
        "base_percentile_queries": {"queries": percentilequeries},
        "percentile_combinations": {"queries": percentile_combinations_queries},
        "mixed_combinations_with_fixed_structure": {
            "queries": mixed_term_combinations_with_fixed_structure_queries,
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
