import csv
import json
import sys
from collections.abc import Generator
from pathlib import Path
import time
from typing import Any

from backend.indices.percentile_op import FainderIndex

import pytest
from loguru import logger

from backend.config import Metadata, Settings


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

    performance_log_dir_fainder = Path("logs/performance_filters_fainder")
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
    # Add CSV log path to pytest's config
    pytest.csv_log_path = csv_log_path_fainder  # type: ignore
    yield

    # Teardown code
    pass  # noqa: PIE790


@pytest.fixture(scope="module")
def fainder() -> FainderIndex:
    settings = Settings()  # type: ignore # uses the environment variables

    fainder_index = FainderIndex(
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
        chunk_layout=settings.fainder_chunk_layout,
        num_workers=settings.max_workers - 1,
        num_chunks=settings.max_workers - 1,
    )
    return fainder_index


@pytest.fixture(scope="module")
def metadata() -> Metadata:
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))
    return metadata
