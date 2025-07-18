#!/usr/bin/env python3
"""
Standalone Fainder Filter Performance Test

This script runs performance tests for Fainder filtering without requiring pytest.
It combines the functionality from the original pytest-based test files.

Usage:
    python standalone_performance_test.py [--num-workers N]

Options:
    --num-workers N    Number of workers for Fainder index (optional)
"""

import argparse
import csv
import json
import sys
from pathlib import Path
import time
from typing import Any

from backend.indices.percentile_op import FainderIndex
from backend.config import Metadata, FainderMode, Settings
from numpy import uint32
import numpy as np
from numpy.typing import ArrayLike, NDArray
from loguru import logger


# Test parameters
REFERENCES = [1, 100, 10000, 1000000, 10000000]
COMPARISONS = ["le", "ge"]
PERCENTILES = [0.1, 0.5, 0.9]
FAINDER_MODES = [FainderMode.FULL_PRECISION, FainderMode.EXACT]

FILTER_SIZES_RIGHT = [100, 10000, 100000, 1000000, 10000000, 30000000, 50000000]
FILTER_SIZES_WRONG = [0, 1000, 10000, 100000, 1000000, 10000000, 30000000, 50000000]


def create_fainder_queries() -> list[dict[str, Any]]:
    """Create all combinations of test parameters."""
    fainder_queries: list[dict[str, Any]] = []
    for reference in REFERENCES:
        for comparison in COMPARISONS:
            for percentile in PERCENTILES:
                for fainder_mode in FAINDER_MODES:
                    fainder_queries.append(
                        {
                            "percentile": percentile,
                            "comparison": comparison,
                            "reference": reference,
                            "fainder_mode": fainder_mode,
                        }
                    )
    return fainder_queries


def setup_logging_and_directories(csv_log_path: Path) -> None:
    """Set up logging and create necessary directories."""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_files_dir = Path("logs/log_files")
    log_files_dir.mkdir(exist_ok=True)

    performance_log_dir_fainder = Path("logs/performance_filters_fainder")
    performance_log_dir_fainder.mkdir(exist_ok=True)

    # Create CSV performance log file and write headers
    with csv_log_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "percentile",
                "comparison",
                "reference",
                "fainder_mode",
                "execution_time",
                "filter_size_right",
                "filter_size_wrong",
                "filter_size",
                "num_results",
                "num_results_without_filter",
                "query",
                "num_workers",
            ]
        )

    # Remove default handler
    logger.remove()

    # Add handlers for both file and console
    logger.add(
        sys.stdout,
        format="{time:HH:mm:ss} | {level: >5} | {file}:{line} | <level>{message}</level>",
        level="DEBUG",
    )
    logger.add(
        "logs/log_files/test_{time:YYYY-MM-DD HH:mm:ss}.log",
        format="{time:HH:mm:ss} | {level: >5} | {file}:{line} | {message}",
        level="DEBUG",
    )


def create_fainder_index(num_workers: int | None = None) -> FainderIndex:
    """Create and configure the Fainder index."""
    settings = Settings()  # type: ignore # uses the environment variables

    # Override num_workers if provided
    if num_workers is not None:
        settings.fainder_num_workers = int(num_workers)

    fainder_index = FainderIndex(
        rebinning_paths={"default": settings.rebinning_index_path},
        conversion_paths={"default": settings.conversion_index_path},
        histogram_path=settings.histogram_path,
        chunk_layout=settings.fainder_chunk_layout,
        num_workers=settings.fainder_num_workers,
        num_chunks=settings.fainder_num_workers,
    )
    return fainder_index


def load_metadata() -> Metadata:
    """Load metadata from settings."""
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))
    return metadata


def log_performance_csv(
    csv_path: Path,
    percentile: float,
    comparison: str,
    reference: int,
    fainder_mode: str,
    filter_size_right: int,
    filter_size_wrong: int,
    execution_time: float,
    filter_size: int,
    results: NDArray[np.uint32],
    results_size_without_filtering: int,
    num_workers: int,
) -> None:
    """Log performance metrics to CSV file."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                timestamp,
                percentile,
                comparison,
                reference,
                fainder_mode,
                execution_time,
                filter_size_right,
                filter_size_wrong,
                filter_size,
                results.size,
                results_size_without_filtering,
                f"pp({percentile};{comparison};{reference})",
                num_workers,
            ]
        )


def run_fainder_search(
    fainder: FainderIndex,
    query: dict[str, Any],
    fainder_mode: FainderMode,
    hist_filter: ArrayLike | None = None,
) -> NDArray[np.uint32]:
    """Run a Fainder search with the given parameters."""
    if hist_filter is not None:
        hist_filter = np.array(hist_filter)
    return fainder.search(
        query["percentile"],
        query["comparison"],
        query["reference"],
        fainder_mode,
        "default",
        hist_filter,
    )


def run_performance_test(
    fainder: FainderIndex,
    metadata: Metadata,
    query: dict[str, Any],
    csv_path: Path,
) -> None:
    """Run performance test for a single query configuration."""
    logger.info(f"Running test for query: {query}")
    
    # Test without filtering first
    start_time = time.perf_counter()
    result_without_filtering = run_fainder_search(
        fainder,
        query,
        query["fainder_mode"],
    )
    time_without_filtering = time.perf_counter() - start_time
    num_of_hists = metadata.num_hists

    # Log baseline performance (no filtering)
    log_performance_csv(
        csv_path,
        query["percentile"],
        query["comparison"],
        query["reference"],
        query["fainder_mode"].value,
        0,
        0,
        time_without_filtering,
        0,
        result_without_filtering,
        result_without_filtering.size,
        fainder.parallel_processor.num_workers if fainder.parallel_processor else 1,
    )

    logger.info(f"Baseline test completed in {time_without_filtering:.4f}s, {result_without_filtering.size} results")

    # Test with different filter sizes
    for filter_size_right in FILTER_SIZES_RIGHT:
        for filter_size_wrong in FILTER_SIZES_WRONG:
            filter_size_right = min(filter_size_right, len(result_without_filtering))
            filter_size_wrong = min(filter_size_wrong, num_of_hists - filter_size_right)

            # Create a filter of the desired size by selecting indices out of the results without filtering
            hist_filter = list(result_without_filtering)[:filter_size_right]

            # Add some indices that are not in the results without filtering
            filter_wrong = list(
                {uint32(x) for x in range(num_of_hists)}
                - {x for x in result_without_filtering}
            )
            filter_wrong = filter_wrong[:filter_size_wrong]
            hist_filter.extend(filter_wrong)

            np_filter = np.array(list(hist_filter))

            start_time = time.perf_counter()
            result_with_filtering = run_fainder_search(
                fainder,
                query,
                query["fainder_mode"],
                np_filter,
            )
            time_with_filtering = time.perf_counter() - start_time

            log_performance_csv(
                csv_path,
                query["percentile"],
                query["comparison"],
                query["reference"],
                query["fainder_mode"].value,
                filter_size_right,
                filter_size_wrong,
                time_with_filtering,
                np_filter.size,
                result_with_filtering,
                result_without_filtering.size,
                fainder.parallel_processor.num_workers
                if fainder.parallel_processor
                else 1,
            )

            logger.debug(f"Filter test: right={filter_size_right}, wrong={filter_size_wrong}, "
                        f"time={time_with_filtering:.4f}s, results={result_with_filtering.size}")


def main() -> None:
    """Main function to run all performance tests."""
    parser = argparse.ArgumentParser(description="Run Fainder filter performance tests")
    parser.add_argument(
        "--num-workers",
        type=int,
        default=None,
        help="Number of workers for Fainder index",
    )
    args = parser.parse_args()

    # Set up logging and directories
    performance_log_dir_fainder = Path("logs/performance_filters_fainder")
    csv_log_path = (
        performance_log_dir_fainder
        / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )
    
    setup_logging_and_directories(csv_log_path)
    
    logger.info("Starting Fainder filter performance tests")
    logger.info(f"Results will be logged to: {csv_log_path}")
    
    # Initialize Fainder and metadata
    logger.info("Initializing Fainder index...")
    fainder = create_fainder_index(args.num_workers)
    metadata = load_metadata()
    
    logger.info(f"Fainder index initialized with {fainder.parallel_processor.num_workers if fainder.parallel_processor else 1} workers")
    logger.info(f"Metadata loaded: {metadata.num_hists} histograms")

    # Generate all test queries
    fainder_queries = create_fainder_queries()
    total_queries = len(fainder_queries)
    
    logger.info(f"Running {total_queries} query configurations...")

    # Run all tests
    for i, query in enumerate(fainder_queries, 1):
        logger.info(f"Progress: {i}/{total_queries} - Testing query {i}")
        try:
            run_performance_test(fainder, metadata, query, csv_log_path)
        except Exception as e:
            logger.error(f"Error running test for query {query}: {e}")
            continue

    logger.info("All performance tests completed!")
    logger.info(f"Results saved to: {csv_log_path}")


if __name__ == "__main__":
    main()
