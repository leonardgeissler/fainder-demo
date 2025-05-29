import csv
import json
import sys
from collections.abc import Generator
from pathlib import Path
import time
from typing import Any

from backend.engine.engine import Engine
from backend.indices.keyword_op import TantivyIndex
from backend.indices.percentile_op import FainderIndex
from backend.indices.name_op import HnswIndex as ColumnIndex

import pytest
from loguru import logger

from backend.config import ExecutorType, Metadata, Settings


@pytest.fixture(autouse=True, scope="module")
def _setup_and_teardown() -> Generator[None, Any, None]:  # pyright: ignore[reportUnusedFunction]
    """
    Generic setup and teardown fixture that runs before and after each test.
    """
    # Setup code

    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_files_dir = Path("logs/log_files")
    log_files_dir.mkdir(exist_ok=True)

    performance_log_dir_fainder = Path("logs/breaking_points_fainder")
    performance_log_dir_fainder.mkdir(exist_ok=True)

    # Create CSV performance log file and write headers
    csv_log_path_fainder = (
        performance_log_dir_fainder
        / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )

    with csv_log_path_fainder.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "fainder_mode",
                "filter_size_wrong",
                "filter_size_right",
                "filter_size",
                "filter_size_wrong_doc",
                "filter_size_right_doc",
                "filter_size_doc",
                "execution_time",
                "execution_time_first",
                "num_results_first",
                "num_results",
                "query",
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
    # Add CSV log path to pytest's config
    pytest.csv_log_path = csv_log_path_fainder  # type: ignore
    yield

    # Teardown code
    pass  # noqa: PIE790


@pytest.fixture(scope="module")
def engines() -> tuple[Engine, Engine, Metadata]:
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    fainder_index = FainderIndex(
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
        parallel=settings.fainder_parallel,
        num_workers=settings.max_workers,
        contiguous=settings.fainder_contiguous_chunks,
    )
    column_index = ColumnIndex(path=settings.hnsw_index_path, metadata=metadata)
    return Engine(
        tantivy_index=TantivyIndex(
            index_path=str(settings.tantivy_path), recreate=False
        ),
        fainder_index=fainder_index,
        hnsw_index=column_index,
        metadata=metadata,
        cache_size=0,
        executor_type=ExecutorType.SIMPLE
    ), Engine(
        tantivy_index=TantivyIndex(
            index_path=str(settings.tantivy_path), recreate=False
        ),
        fainder_index=fainder_index,
        hnsw_index=column_index,
        metadata=metadata,
        cache_size=0,
        executor_type=ExecutorType.PREFILTERING
    ), metadata
