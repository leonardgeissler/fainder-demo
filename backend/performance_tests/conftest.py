import csv
import json
import sys
import time
from pathlib import Path

import pytest
from loguru import logger

from backend.config import ExecutorType, Metadata, Settings
from backend.engine import Engine
from backend.indices.keyword_op import TantivyIndex
from backend.indices.name_op import HnswIndex as ColumnIndex
from backend.indices.percentile_op import FainderIndex


@pytest.fixture(autouse=True, scope="module")
def _setup_and_teardown() -> None:  # pyright: ignore[reportUnusedFunction]
    """
    Generic setup and teardown fixture that runs before and after each test.
    """
    # Setup code

    # Create logs directory if it doesn't exist
    base_log_dir = Path("logs")
    base_log_dir.mkdir(exist_ok=True)
    log_dir = Path("logs/logs")
    log_dir.mkdir(exist_ok=True)

    performance_log_dir = Path("logs/performance")
    performance_log_dir.mkdir(exist_ok=True)

    # Create directory for profiling logs
    profiling_log_dir = Path("logs/profiling")
    profiling_log_dir.mkdir(exist_ok=True)
    profiling_raw_dir = Path("logs/profiling/raw")
    profiling_raw_dir.mkdir(exist_ok=True)

    # Create CSV performance log file and write headers
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")

    csv_log_path = performance_log_dir / f"performance_metrics_{timestamp_str}.csv"
    with csv_log_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "category",
                "test_name",
                "query",
                "scenario",
                "execution_time",
                "results_consistent",
                "fainder_mode",
                "num_results",
                "ids",
                "num_terms",
                "id_str",
            ]
        )

    # Create CSV profiling log file and write headers
    profile_csv_log_path = profiling_log_dir / f"profiling_metrics_{timestamp_str}.csv"
    with profile_csv_log_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "category",
                "test_name",
                "query",
                "scenario",
                "fainder_mode",
                "ncalls",
                "tottime",
                "percall_tottime",
                "cumtime",
                "percall_cumtime",
                "function_info",
            ]
        )

    # Remove default handler
    logger.remove()

    # Add handlers for both file and console
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {message}",
        filter=lambda record: record["level"].name == "INFO",
    )
    logger.add(
        "logs/logs/query_performance__{time:YYYY-MM-DD HH:mm:ss}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {message}",
        filter=lambda record: record["level"].name == "INFO",
        rotation="1 day",
    )

    # Add CSV log paths to pytest's config
    pytest.csv_log_path = csv_log_path  # type: ignore
    pytest.profile_csv_log_path = profile_csv_log_path  # type: ignore

    return

    # Teardown code


@pytest.fixture(scope="module")
def engines() -> tuple[Engine, Engine, Engine, Engine]:
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    fainder_index = FainderIndex(
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
    )
    column_index = ColumnIndex(path=settings.hnsw_index_path, metadata=metadata)
    return (
        Engine(
            tantivy_index=TantivyIndex(index_path=str(settings.tantivy_path), recreate=False),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.SIMPLE,
        ),
        Engine(
            tantivy_index=TantivyIndex(index_path=str(settings.tantivy_path), recreate=False),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.PREFILTERING,
        ),
        Engine(
            tantivy_index=TantivyIndex(index_path=str(settings.tantivy_path), recreate=False),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.PARALLEL,
        ),
        Engine(
            tantivy_index=TantivyIndex(index_path=str(settings.tantivy_path), recreate=False),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.PARALLEL_PREFILTERING,
        ),
    )
