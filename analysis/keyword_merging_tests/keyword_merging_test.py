import csv
import time
from pathlib import Path
from typing import Any

import pytest
from loguru import logger

from backend.engine import Engine
from .generate_keyword_test_cases import generate_keyword_test_cases


def execute_and_time(engine: Engine, query: str) -> tuple[Any, float]:
    """Execute a query and measure execution time."""
    start_time = time.time()
    result, _ = engine.execute(query)
    end_time = time.time()
    return result, end_time - start_time


def run_comparison(
    engines: tuple[Engine, Engine], merged_query: str, unmerged_query: str
) -> tuple[list, list, float, float, bool, int]:
    """
    Run both merged and unmerged versions of the query and compare results.
    Returns:
        merged_results, unmerged_results, merged_time, unmerged_time, is_consistent, term_count
    """
    # Engine with keyword merging enabled
    merged_results, merged_time = execute_and_time(engines[0], merged_query)

    # Engine without keyword merging enabled
    unmerged_results, unmerged_time = execute_and_time(engines[1], unmerged_query)

    # Check if results are consistent
    is_consistent = set(merged_results) == set(unmerged_results)

    # Count terms in unmerged query (by counting 'kw(' occurrences)
    term_count = unmerged_query.count("kw(")

    return (
        merged_results,
        unmerged_results,
        merged_time,
        unmerged_time,
        is_consistent,
        term_count,
    )


def log_performance_csv(
    csv_path: Path,
    timestamp: str,
    merged_query: str,
    merged_time: float,
    unmerged_time: float,
    is_consistent: bool,
    term_count: int,
    merged_result_count: int,
    unmerged_result_count: int,
) -> None:
    """Log performance data to CSV file."""
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                merged_query,
                merged_time,
                unmerged_time,
                is_consistent,
                term_count,
                merged_result_count,
                unmerged_result_count,
            ]
        )


@pytest.mark.parametrize(
    ("test_name", "merged_query", "unmerged_query"), generate_keyword_test_cases()
)
def test_keyword_merging(
    test_name: str,
    merged_query: str,
    unmerged_query: str,
    engine: tuple[Engine, Engine],
) -> None:
    """Test the performance difference between merged and unmerged keyword queries."""
    # Run comparison
    (
        merged_results,
        unmerged_results,
        merged_time,
        unmerged_time,
        is_consistent,
        term_count,
    ) = run_comparison(engine, merged_query, unmerged_query)

    # Log to CSV
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    csv_path = Path(pytest.csv_log_path)  # type: ignore
    log_performance_csv(
        csv_path,
        timestamp,
        merged_query,
        merged_time,
        unmerged_time,
        is_consistent,
        term_count,
        len(merged_results),
        len(unmerged_results),
    )

    # Create detailed performance log
    performance_log = {
        "test_name": test_name,
        "merged_query": merged_query,
        "unmerged_query": unmerged_query,
        "metrics": {
            "merged_time": merged_time,
            "unmerged_time": unmerged_time,
            "speedup": unmerged_time / merged_time if merged_time > 0 else float("inf"),
        },
        "result_count": len(merged_results),
        "results_consistent": is_consistent,
        "term_count": term_count,
    }

    logger.info("PERFORMANCE_DATA: " + str(performance_log))

    # Assert that results are consistent
    assert set(merged_results) == set(unmerged_results), (
        "Results are inconsistent between merged and unmerged queries"
    )
