import json
from itertools import combinations
from pathlib import Path
from typing import Any

from loguru import logger

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
            "ids": [f"'{field_prefix}{word}'"],
            "num_terms": 1,
        }
        for i, word in enumerate(keywords)
    }


def generate_simple_keyword_queries_with_multiple_elements(
    keywords: list[str],
    num_elements: int = 5,
    max_num_queries: int = 10,
) -> dict[str, dict[str, Any]]:
    """
    Generate simple keyword queries with multiple elements.

    Args:
        keywords: List of keywords to search for
        num_elements: Number of elements to combine in each query
        max_num_queries: Maximum number of queries to generate
    """

    queries: dict[str, dict[str, Any]] = {}
    query_counter = 0

    combinations_list = list(combinations(keywords, num_elements))
    for combination in combinations_list:
        if query_counter >= max_num_queries:
            break
        query_counter += 1
        query = " OR ".join([f"kw('{word}')" for word in combination])
        queries[f"multi_element_query_{query_counter}"] = {
            "query": query,
            "ids": [{"keyword_id": word} for word in combination],
            "num_terms": num_elements,
        }

    return queries

def generate_simple_column_name_queries(
    column_names: list[str],
    ks: list[int],
    max_num_queries: int = 10,
) -> dict[str, dict[str, Any]]:
    queries: dict[str, dict[str, Any]] = {}
    query_counter = 0

    for col in column_names:
        for k in ks:
            query_counter += 1
            if query_counter > max_num_queries:
                break
            queries[f"column_query_{col}_{k}"] = {
                "query": f"col(name('{col}';{k}))",
                "ids": [{"column_id": col}],
                "num_terms": 1,
            }

    return queries

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
    max_terms: int = 10,
) -> dict[str, dict[str, Any]]:
    """
    Generate percentile queries for testing.

    Args:
        percentile_values: List of percentile values
        thresholds: List of threshold values
        operators: List of operators to use
    """

    queries: dict[str, dict[str, Any]] = {}
    query_counter = 0

    for op in operators:
        for i, threshold in enumerate(thresholds):
            for h, percentile in enumerate(percentile_values):
                query_counter += 1
                if query_counter > max_terms:
                    break
                queries[f"percentile_{op}_{i}_{h}"] = {
                    "query": f"col(pp({percentile};{op};{threshold}))",
                    "percentile_id": f"pp({percentile};{op};{threshold})",
                    "ids": [{"percentile_id": f"pp({percentile};{op};{threshold})"}],
                    "num_terms": 1,
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
                    "num_terms": num_terms,
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
                    "ids": num_terms,
                    "num_terms": num_terms,
                }
                query_counter += 1
                i += 1
                if i > num_query_per_num_terms:
                    break
    return queries


def expected_form(
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
                            "num_terms": 3,
                        }
                        query_counter += 1
                        if query_counter > max_terms:
                            return queries

    return queries


def multiple_percentile_combinations(
    list_of_percentile_combinations: dict[int, dict[str, dict[str, Any]]],
    operators: list[str],
    min_terms: int ,
    max_terms: int ,
    num_query_per_num_terms: int,
) -> dict[str, dict[str, Any]]:
    """
    Combines multiple different percentile combinations into a single query. With same number of terms in percentile combinations.

    Args:
        list_of_percentile_combinations: List of percentile combinations
        operators: List of logical operators to use
    """
    queries: dict[str, dict[str, Any]] = {}

    min_terms_internal = min(list(list_of_percentile_combinations.keys()))
    max_terms_internal = max(list(list_of_percentile_combinations.keys()))
    for operator in operators:
        for num_terms_external in range(min_terms, max_terms + 1):
            for num_terms_internal in range(min_terms_internal, max_terms_internal + 1):
                internalterms = list_of_percentile_combinations.get(num_terms_internal, {})
                if not internalterms:
                    continue
                query_counter = 0
                for combination in combinations(internalterms.keys(), num_terms_external):
                    if len(combination) == 0:
                        continue
                    combination_terms = [
                        internalterms[term]["query"] for term in combination
                    ]
                    query = f" {operator} ".join(combination_terms)
                    query = wrap_term(query)
                    queries[f"multiple_percentile_combination_{operator}_{num_terms_external}_{query_counter}"] = {
                        "query": query,
                        "ids": {"num_terms_external": num_terms_external, 
                                "num_terms_internal": num_terms_internal,
                                "num_terms": num_terms_external * num_terms_internal,
                                },
                        "num_terms": num_terms_external * num_terms_internal,
                    }
                    query_counter += 1
                    if query_counter > num_query_per_num_terms:
                        break
                if query_counter > num_query_per_num_terms:
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
                            "num_terms": 3,
                        }
                        query_counter += 1
                        if query_counter > max_terms:
                            return queries
    return queries


def double_expected_form( 
    keywords: list[str],
    terms: list[str],
    column_names: list[str],
    ks: list[int],
    max_terms: int,
) -> dict[str, dict[str, Any]]:
    """
    Generate expected form for double queries.

    Args:
        keywords: List of keywords to combine
        terms: List of terms to combine
        column_names: List of column names to combine
        ks: List of ks to combine
    """

    # query structure: (kw('test') AND col(name('age',1) AND pp(..) AND col(name('date',1) AND pp(..))) OR (kw('test2') AND col(name('age',1) AND pp(..) AND col(name('date',1) AND pp(..))))
    queries: dict[str, dict[str, Any]] = {}
    query_counter = 1
    # get 2 keywords (different)
    # get 4 column names (different)
    # get 4 ks (can be the same)
    def get_unique_pairs(items):
        return [(a, b) for i, a in enumerate(items) for b in items[i+1:]]

    # Get unique pairs of keywords, column names
    for kw1, kw2 in get_unique_pairs(keywords):
        # Get two pairs of unique column names
        for (col1, col2), (col3, col4) in zip(get_unique_pairs(column_names), get_unique_pairs(column_names)):
            # Get unique k values 
            for k1, k2 in get_unique_pairs(ks):
                # Get unique term combinations
                for term1, term2, term3, term4 in [
                    (t1,t2,t3,t4) 
                    for i,t1 in enumerate(terms)
                    for j,t2 in enumerate(terms[i+1:], i+1) 
                    for k,t3 in enumerate(terms[j+1:], j+1)
                    for t4 in terms[k+1:]
                ]:
                    query = (
                        f"(kw('{kw1}') AND "
                        f"col(name('{col1}';{k1}) AND {term1}) AND "
                        f"col(name('{col2}';{k2}) AND {term2})) OR "
                        f"(kw('{kw2}') AND "
                        f"col(name('{col3}';{k1}) AND {term3}) AND " 
                        f"col(name('{col4}';{k2}) AND {term4}))"
                    )

                    queries[f"double_expected_form_{query_counter}"] = {
                        "query": query,
                        "ids": [
                            {"keyword_id": kw1},
                            {"percentile_id": term1}, 
                            {"percentile_id": term2},
                            {"column_id": (col1, k1)},
                            {"keyword_id": kw2},
                            {"percentile_id": term3},
                            {"percentile_id": term4}, 
                            {"column_id": (col3, k1)},
                            {"column_id": (col4, k2)},
                        ],
                        "num_terms": 7,
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

        new_queries["early_exit_" + form + "_" + query_name] = {
            "query": "kw('agjkehkejhgkjehgsjkhg') AND "+ query["query"],
            "ids": query["ids"],
        }

    return new_queries


def multiple_percentile_combinations_with_kw(
    keywords: list[str],
    multiple_percentile_combinations: dict[str, dict[str, Any]],
    num_kws: int = 1,
):
    # add keyword to each query
    queries: dict[str, dict[str, Any]] = {}
    keywords = keywords[:num_kws]  # Limit to num_kws keywords
    for query_name, query in multiple_percentile_combinations.items():
        for keyword in keywords:
            queries[query_name] = {
                "query": f"kw('{keyword}') AND ({query['query']})",
                "ids": query["ids"],
            }
    return queries

def expected_form_not(
    keywords: list[str],
    terms: list[str],
    column_names: list[str],
    ks: list[int],
    operators: list[str],
    max_terms: int,
) -> dict[str, dict[str, Any]]:

    # expected form: kw('test') AND col(name('age',1) AND pp(0.5;le;2000))
    # with NOT operator added to all possible combinations
    queries: dict[str, dict[str, Any]] = {}
    query_counter = 1
    helper = [0, 1, 2, 3]
    combination_1 = list(combinations(helper, 1))
    combination_2 = list(combinations(helper, 2))
    combination_3 = list(combinations(helper, 3))
    combination_4 = list(combinations(helper, 4))
    combination = combination_1.copy()
    combination.extend(combination_2)
    combination.extend(combination_3)
    combination.extend(combination_4)
    logger.info(f"combinations: {combination}")
    # if 0 then not kw('test') usw
    for operator in operators:
        for keyword in keywords:
            for percentile in terms:
                for column_name in column_names:
                    for k in ks:
                        for comb in combination:
                            if len(comb) == 0:
                                continue
                            query = ""

                            
                            if 0 in comb:
                                query += f"NOT kw('{keyword}') {operator} "
                            else:
                                query += f"kw('{keyword}') {operator} "

                            if 1 in comb:
                                query += f"NOT col("
                            else:
                                query += f"col("
                            
                            if 2 in comb:
                                query += f"NOT name('{column_name}';{k}) {operator} "
                            else:
                                query += f"name('{column_name}';{k}) {operator} "
                            
                            if 3 in comb:
                                query += f"NOT {percentile})"
                            else:
                                query += f"{percentile})"

                            not_ids: list[str] = [str(term) for term in comb]
                            not_ids_str = "-".join(not_ids)

                            queries[f"mixed_combination_{operator}_{query_counter}_{not_ids_str}"] = {
                                "query": query,
                                "ids": not_ids_str,

                            }
                        query_counter += 1
                        if query_counter > max_terms:
                            return queries
    return queries
                        

def middle_exit(
    keywords: list[str],
    terms: list[str],
    max_terms: int,
) -> dict[str, dict[str, Any]]:
    """
    Ads an in the middle of a long query 
    """
    queries: dict[str, dict[str, Any]] = {}
    query_counter = 1
    
    # For each keyword, generate combinations of 10 unique terms
    for keyword in keywords:
        for terms_combination in combinations(terms, 10):
            # Unpack the 10 terms
            term, term2, term3, term4, term5, term6, term7, term8, term9, term10 = terms_combination

            query = f"kw('{keyword}') AND col({term} AND NOT {term}) AND col(NOT {term2} AND NOT {term3} AND NOT {term4} AND NOT {term5} AND NOT {term6} AND NOT {term7} AND NOT {term8} AND NOT {term9} AND NOT {term10})"
            queries[f"middle_exit_{query_counter}"] = {
                "query": query,
                "ids": [
                    {"keyword_id": keyword},
                    {"percentile_id": f"{term}-{term2}-{term3}-{term4}-{term5}-{term6}-{term7}-{term8}-{term9}-{term10}"},
                ],
            }
            query_counter += 1
            if query_counter > max_terms:
                return queries
    return queries


def generate_all_test_cases(config: PerformanceConfig) -> dict[str, Any]:
    """
    Generate all test cases, optionally using a configuration.
    
    Args:
        config: Optional config object (pydantic model or dict)
    """

    # Generate test cases with the config values
    keywordsqueries = generate_simple_keyword_queries(config.keywords.default_keywords)

    keywordsqueries_with_multiple_elements = generate_simple_keyword_queries_with_multiple_elements(
        keywords=config.keywords.default_keywords,
        num_elements=config.query_generation.num_elements_keywordsqueries,
        max_num_queries=config.query_generation.max_num_keywordsqueries,
    )

    base_column_name_queries = generate_simple_column_name_queries(
        column_names=config.keywords.default_col_names,
        ks=config.keywords.default_ks,
        max_num_queries=config.query_generation.max_num_column_name_queries,
    )

    percentile_terms_list = generate_percentile_terms(
        percentile_values=config.percentiles.default_percentiles,
        thresholds=config.percentiles.default_thresholds,
        operators=config.percentiles.default_operators,
    )
    percentilequeries = generate_percentile_queries(
        percentile_values=config.percentiles.default_percentiles,
        thresholds=config.percentiles.default_thresholds,
        operators=config.percentiles.default_operators,
        max_terms=config.query_generation.max_num_terms_percentilequeries
    )

    percentile_combinations_queries = percentile_term_combinations(
        terms=percentile_terms_list,
        operators=config.keywords.logical_operators,
        min_terms=config.query_generation.min_num_terms_percentile_combinations,
        max_terms=config.query_generation.max_num_terms_percentile_combinations,
        num_query_per_num_terms=config.query_generation.max_num_query_per_term_count_percentile_combinations,
    )
    list_of_percentile_combinations: dict[int, dict[str, dict[str, Any]]] = {}
    for i in range(
        config.query_generation.min_num_terms_percentile_combinations,
        config.query_generation.max_num_terms_percentile_combinations + 1,
    ):
        list_of_percentile_combinations[i] = percentile_term_combinations(
            terms=percentile_terms_list,
            operators=config.keywords.logical_operators,
            min_terms=i,
            max_terms=i,
            num_query_per_num_terms=config.query_generation.max_num_query_per_term_count_percentile_combinations,
        )

    mixed_term_combinations_with_fixed_structure_queries = (
        expected_form(
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
        list_of_percentile_combinations=list_of_percentile_combinations,
        operators=config.keywords.logical_operators,
        min_terms=config.query_generation.min_num_terms_multiple_percentile_combinations,
        max_terms=config.query_generation.max_num_terms_multiple_percentile_combinations,
        num_query_per_num_terms=config.query_generation.max_num_query_per_term_count_multiple_percentile_combinations,
    )
    
    multiple_percentile_combinations_queries_or = multiple_percentile_combinations(
        list_of_percentile_combinations=list_of_percentile_combinations,
        operators=["OR"],
        min_terms=config.query_generation.min_num_terms_percentile_combinations,
        max_terms=config.query_generation.max_num_terms_percentile_combinations,
        num_query_per_num_terms=config.query_generation.max_num_query_per_term_count_multiple_percentile_combinations,
    )

    multiple_percentile_combinations_queries_with_kw = (
        multiple_percentile_combinations_with_kw(
            keywords=config.keywords.default_keywords,
            multiple_percentile_combinations=multiple_percentile_combinations_queries_or,
            num_kws=config.query_generation.num_of_ḱws_for_multiple_pp_combinations,
        )
    )

    expected_form_not_queries = expected_form_not(
        keywords=config.keywords.default_keywords,
        terms=percentile_terms_list,
        column_names=config.keywords.default_col_names,
        ks=config.keywords.default_ks,
        operators=config.keywords.logical_operators,
        max_terms=config.query_generation.max_num_mixed_terms_with_fixed_structure_not,
    )

    double_expected_form_queries = double_expected_form(
        keywords=config.keywords.default_keywords,
        terms=percentile_terms_list,
        column_names=config.keywords.default_col_names,
        ks=config.keywords.default_ks,
        max_terms=config.query_generation.max_num_terms_double_expected_form,
    )

    middle_exit_queries = middle_exit(
        keywords=config.keywords.default_keywords,
        terms=percentile_terms_list,
        max_terms=config.query_generation.max_num_middle_exit,  
    )


    test_cases = {
        "base_keyword_queries": {"queries": keywordsqueries},
        "base_keyword_queries_with_multiple_elements": {
            "queries": keywordsqueries_with_multiple_elements
        },
        "base_column_name_queries": {"queries": base_column_name_queries},
        "base_percentile_queries": {"queries": percentilequeries},
        "Percentile Combinations": {"queries": percentile_combinations_queries},
        "Multiple Percentile Combinations": {"queries": multiple_percentile_combinations_queries},
        "Expected Form": {
            "queries": mixed_term_combinations_with_fixed_structure_queries
        },
        "Expected Form Extended": {
            "queries": mixed_term_combinations_with_fixed_structure_extented_queries
        },
        "Multiple percentile combinations with Keyword": {
            "queries": multiple_percentile_combinations_queries_with_kw
        },
        "Double expected Form": {
            "queries": double_expected_form_queries
        },
        "Early empty Results": {"queries": early_exit_queries},
        "Middle empty Results": {
            "queries": middle_exit_queries
        },
        "NOT Combinations": {
            "queries": expected_form_not_queries
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
