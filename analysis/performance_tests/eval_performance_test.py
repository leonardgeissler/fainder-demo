import cProfile
import csv
import io
import pstats
import time
from pathlib import Path
from typing import Any

from loguru import logger

from backend.engine import Engine


from pstats import SortKey


def execute_with_profiling(
    evaluator: Engine, query: str, params: dict[str, Any], mode: str, disable_profiling: bool = True
) -> tuple[Any, float, io.StringIO]:
    """Execute a query with profiling and timing."""
    if disable_profiling:
        start_time = time.time()
        result, _ = evaluator.execute(query, fainder_mode=mode)
        end_time = time.time()
        return result, end_time - start_time, io.StringIO()
    
    pr = cProfile.Profile()
    pr.enable()

    start_time = time.time()
    result, _ = evaluator.execute(query, fainder_mode=mode)
    end_time = time.time()

    pr.disable()

    # Capture profiling output to a StringIO object
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s)
    ps.sort_stats(SortKey.TIME)
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
    function_stats: list[dict[str, Any]] = []
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
    with debug_file.open("w") as f:
        f.write(stats_str)


def run_evaluation_scenarios(
    query: str,
    scenarios: dict[str, dict[str, Any]],
    mode: str,
    profile_csv_path: Path,
    category: str,
    test_name: str,
    disable_profiling: bool = True,
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
        result, execution_time, stats_io = execute_with_profiling(evaluator, query, params, mode, disable_profiling)
        timings[scenario_name] = execution_time
        results[scenario_name] = result

        # Save profiling statistics
        if not disable_profiling:
            save_profiling_stats(
                stats_io, profile_csv_path, category, test_name, query, scenario_name, mode
            )

    # Check if all results are consistent
    first_result = next(iter(results.values()))
    is_consistent = all(set(result) == set(first_result) for result in results.values())

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
    ids: list[dict[str, str]] | int ,
    num_terms: int,
    id_str: str,
    write_groups_used: dict[int, int],
    write_groups_actually_used: dict[int, int],
    fainder_parallel: bool = True,
    fainder_max_workers: int = 0,
    fainder_contiguous_chunks: bool = True,
    optimizer_cost_sorting: bool = True,
    optimizer_keyword_merging: bool = True,
    optimizer_split_up_junctions: bool = True,
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
                    num_terms,
                    id_str,
                    write_groups_used,
                    write_groups_actually_used,
                    fainder_parallel,
                    fainder_max_workers,
                    fainder_contiguous_chunks,
                    optimizer_cost_sorting,
                    optimizer_keyword_merging,
                    optimizer_split_up_junctions
                ]
            )
