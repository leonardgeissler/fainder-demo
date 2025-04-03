import cProfile
import csv
import io
import pstats
import time
from pathlib import Path
from typing import Any

import pytest
from loguru import logger

from backend.engine import Engine

from .constants import FAINDER_MODES
from .generate_eval_test_cases import generate_all_test_cases

TEST_CASES = generate_all_test_cases()


def execute_with_profiling(
    evaluator: Engine, query: str, params: dict[str, Any], mode: str
) -> tuple[Any, float, io.StringIO]:
    """Execute a query with profiling and timing."""
    pr = cProfile.Profile()
    pr.enable()

    start_time = time.time()
    result, _ = evaluator.execute(query, fainder_mode=mode)
    end_time = time.time()

    pr.disable()

    # Capture profiling output to a StringIO object
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s)
    ps.sort_stats("cumulative")
    ps.print_stats(20)  # Print top 20 functions

    return result, end_time - start_time, s


def save_profiling_stats(
    stats_io: io.StringIO,
    csv_path: Path,
    category: str,
    test_name: str,
    query: str,
    scenario: str,
    fainder_mode: str,
) -> None:
    """Save profiling statistics to a CSV file."""
    # Get the profiling output as text
    stats_str = stats_io.getvalue()
    lines = stats_str.strip().split("\n")

    # Extract function data
    function_stats = []
    header_found = False
    for line in lines:
        # Skip lines until we find the header
        if not header_found:
            if line.strip().startswith("ncalls"):
                header_found = True
            continue

        # Skip empty lines or totals
        if not line.strip() or ":" not in line:
            continue

        # Extract stats
        try:
            # Remove leading/trailing whitespace and split by whitespace
            parts = line.strip().split(None, 5)  # Split into at most 6 parts

            if len(parts) >= 6:
                ncalls = parts[0]
                tottime = parts[1]
                percall1 = parts[2]
                cumtime = parts[3]
                percall2 = parts[4]
                func_info = parts[5]

                function_stats.append(
                    {
                        "ncalls": ncalls,
                        "tottime": tottime,
                        "percall1": percall1,
                        "cumtime": cumtime,
                        "percall2": percall2,
                        "func_info": func_info,
                    }
                )
        except Exception as e:
            # Log parsing errors but continue
            logger.error(f"Error parsing profiling line: {line} - {e!s}")

    # Write to CSV
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)

        for stat in function_stats:
            writer.writerow(
                [
                    timestamp,
                    category,
                    test_name,
                    query,
                    scenario,
                    fainder_mode,
                    stat["ncalls"],
                    stat["tottime"],
                    stat["percall1"],
                    stat["cumtime"],
                    stat["percall2"],
                    stat["func_info"],
                ]
            )

    # Also write the raw profiling output to a separate file for debugging
    debug_dir = csv_path.parent / "raw"
    debug_dir.mkdir(exist_ok=True)

    debug_file = (
        debug_dir
        / f"profile_{category}_{test_name}_{scenario}_{fainder_mode}_{int(time.time())}.txt"
    )
    with open(debug_file, "w") as f:
        f.write(stats_str)


def run_evaluation_scenarios(
    query: str,
    scenarios: dict[str, dict[str, Any]],
    mode: str,
    profile_csv_path: Path,
    category: str,
    test_name: str,
) -> tuple[dict[str, float], dict[str, list[str]], bool]:
    """
    Run multiple evaluation scenarios for a query and return timing results.
    Returns (timings, results, is_consistent)
    """
    timings: dict[str, float] = {}
    results: dict[str, list[str]] = {}

    for scenario_name, params in scenarios.items():
        evaluator = params["engine"]
        assert isinstance(evaluator, Engine)
        result, execution_time, stats_io = execute_with_profiling(evaluator, query, params, mode)
        timings[scenario_name] = execution_time
        results[scenario_name] = result

        # Save profiling statistics
        save_profiling_stats(
            stats_io, profile_csv_path, category, test_name, query, scenario_name, mode
        )

    # Check if all results are consistent
    first_result = next(iter(results.values()))
    is_consistent = all(result == first_result for result in results.values())

    return timings, results, is_consistent


def log_performance_csv(
    csv_path: Path,
    category: str,
    test_name: str,
    query: str,
    timings: dict[str, float],
    results: dict[str, Any],
    is_consistent: bool,
    fainder_mode: str,
    ids: list[dict[str, str]],
    id_str: str,
) -> None:
    """Log performance data to CSV file with one row per scenario."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        for scenario, execution_time in timings.items():
            writer.writerow(
                [
                    timestamp,
                    category,
                    test_name,
                    query,
                    scenario,
                    execution_time,
                    is_consistent,
                    fainder_mode,
                    len(results[scenario]),
                    ids,
                    len(ids),
                    id_str,
                ]
            )


@pytest.mark.parametrize(
    ("category", "test_name", "test_case"),
    [
        (cat, name, case)
        for cat, data in TEST_CASES.items()
        for name, case in data["queries"].items()
    ],
)
def test_performance(
    category: str,
    test_name: str,
    test_case: dict[str, Any],
    engines: tuple[Engine, Engine, Engine],
) -> None:
    simple_engine, perfiltering_engine, parallel_engine = engines
    query = test_case["query"]
    ids = test_case.get("ids", [])
    keyword_id = test_case.get("keyword_id")
    percentile_id = test_case.get("percentile_id")
    id_str = ""
    if keyword_id:
        id_str = keyword_id
    elif percentile_id:
        id_str = percentile_id

    # Define different evaluation scenarios
    evaluation_scenarios = {
        "simple": {"engine": simple_engine},
        "perfiltering": {"engine": perfiltering_engine},
        "parallel": {"engine": parallel_engine},
    }

    for mode in FAINDER_MODES:
        # Get paths for logs
        csv_path = Path(pytest.csv_log_path)  # type: ignore
        profile_csv_path = Path(pytest.profile_csv_log_path)  # type: ignore

        # Run all scenarios
        timings, results, is_consistent = run_evaluation_scenarios(
            query, evaluation_scenarios, mode, profile_csv_path, category, test_name
        )

        # Log to CSV
        log_performance_csv(
            csv_path,
            category,
            test_name,
            query,
            timings,
            results,
            is_consistent,
            mode,
            ids,
            id_str,
        )

        # Create detailed performance log for console/file
        performance_log = {
            "category": category,
            "test_name": test_name,
            "query": query,
            "metrics": {
                "timings": timings,
            },
            "results_consistent": is_consistent,
        }

        logger.info("PERFORMANCE_DATA: " + str(performance_log))

        # Assert that all results are consistent and name
        first_result = next(iter(results.values()))
        for name, result in results.items():
            assert len(result) == len(first_result), f"Results for {name} have different lengths"
            assert set(result) == set(first_result), f"Results for {name} are inconsistent"
            # assert result == first_result, f"Results for {name} are inconsistent in order"
