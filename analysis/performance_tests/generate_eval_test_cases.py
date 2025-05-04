import json
from itertools import combinations
from pathlib import Path
from typing import Any

from .config_models import PerformanceConfig


def generate_simple_keyword_queries(
    keywords: list[str],
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
    percentile_values: list[float],
    thresholds: list[int],
    operators: list[str],
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
    percentile_values: list[float],
    thresholds: list[int],
    operators: list[str],
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
    operators: list[str],
    min_terms: int,
    max_terms: int,
    num_query_per_num_terms: int,
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
    operators: list[str],
    min_terms: int,
    max_terms: int,
    num_query_per_num_terms: int,
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
    column_names: list[str],
    ks: list[int],
    operators: list[str],
    max_terms: int,
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
                        if query_counter > max_terms:
                            return queries

    return queries


def multiple_percentile_combinations(
    percentile_combinations: dict[str, dict[str, Any]],
    operators: list[str],
    min_terms: int ,
    max_terms: int ,
    num_query_per_num_terms: int,
) -> dict[str, dict[str, Any]]:
    """
    Combines multiple different percentile combinations into a single query.

    Args:
        percentile_combinations: Dictionary of percentile combinations
        operators: List of logical operators to use
    """
    queries: dict[str, dict[str, Any]] = {}

    for operator in operators:
        for i in range(min_terms, max_terms + 1):
            h = 0
            for j in range(1, len(percentile_combinations) + 1):
                if j < i:
                    continue
                combination = list(combinations(percentile_combinations.keys(), j))
                for k in range(len(combination)):
                    if len(combination[k]) == 0:
                        continue
                    combination_terms = [
                        percentile_combinations[term]["query"] for term in combination[k]
                    ]
                    query = f" {operator} ".join(combination_terms)
                    query = wrap_term(query)
                    queries[f"multiple_percentile_combination_{operator}_{i}_{k}"] = {
                        "query": query,
                        "ids": [percentile_combinations[term]["ids"] for term in combination[k]],
                    }
                    h += 1
                    if h > num_query_per_num_terms:
                        break
                if h > num_query_per_num_terms:
                    break

    return queries


def expected_form_extented(
    keywords: list[str],
    terms: list[str],
    column_names: list[str],
    ks: list[int],
    max_terms: int,
) -> dict[str, dict[str, Any]]:
    """
    Generate expected form for extended queries.

    Args:
        keywords: List of keywords to combine
        terms: List of terms to combine
        column_names: List of column names to combine
        ks: List of ks to combine
    """

    # query structure: kw('test') AND col(name('age',1) AND (pp(0.5;le;2000 OR pp(0.5;ge;2000)))
    queries: dict[str, dict[str, Any]] = {}
    query_counter = 1
    for keyword in keywords:
        for column_name in column_names:
            for k in ks:
                for term1 in terms:
                    for term2 in terms:
                        if term1 == term2:
                            continue
                        
                        query = (
                            f"kw('{keyword}') AND "
                            f"col(name('{column_name}';{k}) AND "
                            f"({term1} OR {term2}))"
                        )
                        queries[f"expected_form_{query_counter}"] = {
                            "query": query,
                            "ids": [
                                {"keyword_id": keyword},
                                {"percentile_id": term1},
                                {"percentile_id": term2},
                                {"column_id": (column_name, k)},
                            ],
                        }
                        query_counter += 1
                        if query_counter > max_terms:
                            return queries
    return queries


def early_exit(
    queries: dict[str, dict[str, Any]],
    form: str = "expected_form",
) -> dict[str, dict[str, Any]]:
    """
    Ads an early 0 results to each query
    """

    new_queries: dict[str, dict[str, Any]] = {}
    for query_name, query in queries.items():

        new_queries["early_exit_" + form] = {
            "query": "(kw(a) AND NOT kw(a)) AND"+ query["query"],
            "ids": query["ids"],
        }

    return new_queries


def multiple_percentile_combinations_with_kw(
    keyword: str,
    multiple_percentile_combinations: dict[str, dict[str, Any]],
):
    # add keyword to each query
    queries: dict[str, dict[str, Any]] = {}
    for query_name, query in multiple_percentile_combinations.items():
        queries[query_name] = {
            "query": f"kw('{keyword}') AND {query['query']}",
            "ids": [{"keyword_id": keyword}] + query["ids"],
        }
    return queries


def generate_all_test_cases(config: PerformanceConfig) -> dict[str, Any]:
    """
    Generate all test cases, optionally using a configuration.
    
    Args:
        config: Optional config object (pydantic model or dict)
    """

    # Generate test cases with the config values
    keywordsqueries = generate_simple_keyword_queries(config.keywords.default_keywords)
    percentile_terms_list = generate_percentile_terms(
        percentile_values=config.percentiles.default_percentiles,
        thresholds=config.percentiles.default_thresholds,
        operators=config.percentiles.default_operators,
    )
    percentilequeries = generate_percentile_queries(
        percentile_values=config.percentiles.default_percentiles,
        thresholds=config.percentiles.default_thresholds,
        operators=config.percentiles.default_operators,
    )

    percentile_combinations_queries = percentile_term_combinations(
        terms=percentile_terms_list,
        operators=config.keywords.logical_operators,
        min_terms=config.query_generation.min_num_terms_query,
        max_terms=config.query_generation.max_num_terms_query,
        num_query_per_num_terms=config.query_generation.max_num_query_per_num_terms,
    )

    mixed_term_combinations_with_fixed_structure_queries = (
        mixed_term_combinations_with_fixed_structure(
            keywords=config.keywords.default_keywords,
            terms=percentile_terms_list,
            column_names=config.keywords.default_col_names,
            ks=config.keywords.default_ks,
            operators=config.keywords.logical_operators,
            max_terms=config.query_generation.max_num_mixed_terms_with_fixed_structure,
        )
    )

    mixed_term_combinations_with_fixed_structure_extented_queries = (
        expected_form_extented(
            keywords=config.keywords.default_keywords,
            terms=percentile_terms_list,
            column_names=config.keywords.default_col_names,
            ks=config.keywords.default_ks,
            max_terms=config.query_generation.max_num_mixed_terms_extended_with_fixed_structure,

        )
    )

    early_exit_queries = early_exit(
        mixed_term_combinations_with_fixed_structure_extented_queries,
        form="expected_form",
    )

    multiple_percentile_combinations_queries = multiple_percentile_combinations(
        percentile_combinations=percentile_combinations_queries,
        operators=config.keywords.logical_operators,
        min_terms=config.query_generation.min_num_terms_query,
        max_terms=config.query_generation.max_num_terms_query,
        num_query_per_num_terms=config.query_generation.max_num_query_per_num_terms,
    )
    
    multiple_percentile_combinations_queries_or = multiple_percentile_combinations(
        percentile_combinations=percentile_combinations_queries,
        operators=["OR"],
        min_terms=config.query_generation.min_num_terms_query,
        max_terms=config.query_generation.max_num_terms_query,
        num_query_per_num_terms=config.query_generation.max_num_query_per_num_terms,

    )
    
    multiple_percentile_combinations_queries_with_kw = (
        multiple_percentile_combinations_with_kw(
            keyword=config.keywords.default_keywords[0],
            multiple_percentile_combinations=multiple_percentile_combinations_queries_or
        )
    )

    test_cases = {
        "base_keyword_queries": {"queries": keywordsqueries},
        "base_percentile_queries": {"queries": percentilequeries},
        "percentile_combinations": {"queries": percentile_combinations_queries},
        "mixed_combinations_with_fixed_structure": {
            "queries": mixed_term_combinations_with_fixed_structure_queries
        },
        "mixed_combinations_with_fixed_structure_extented": {
            "queries": mixed_term_combinations_with_fixed_structure_extented_queries
        },
        "early_exit": {"queries": early_exit_queries},
        "multiple_percentile_combinations": {"queries": multiple_percentile_combinations_queries},
        "multiple_percentile_combinations_with_kw": {
            "queries": multiple_percentile_combinations_queries_with_kw
        },
    }

    # Filter test cases based on enabled_tests
    test_cases = {name: data for name, data in test_cases.items() if name in config.experiment.enabled_tests}

    output = Path("test_cases/performance_test_cases.json")
    output.parent.mkdir(exist_ok=True)
    with output.open("w") as f:
        json.dump(test_cases, f, indent=2)

    return test_cases


def save_test_cases(output_path: Path, config: PerformanceConfig | None = None) -> None:
    """Generate test cases and save them to a JSON file."""
    if config is None:
        config = PerformanceConfig()
    test_cases = generate_all_test_cases(config)
    with output_path.open("w") as f:
        json.dump(test_cases, f, indent=2)


if __name__ == "__main__":
    output_dir = Path("test_cases")
    output_dir.mkdir(exist_ok=True)
    save_test_cases(output_dir / "performance_test_cases.json")
