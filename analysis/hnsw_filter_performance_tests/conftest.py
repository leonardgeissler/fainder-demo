import csv
import json
import sys
from collections.abc import Generator
from pathlib import Path
import time
from typing import Any

from backend.indices.name_op import HnswIndex
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

    performance_log_dir_hnsw = Path("logs/performance_filters_hnsw")
    performance_log_dir_hnsw.mkdir(exist_ok=True)

    csv_log_path_hnsw = (
        performance_log_dir_hnsw
        / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )

    with csv_log_path_hnsw.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "column_name",
                "k",
                "execution_time",
                "filter_size_right",
                "filter_size_wrong",
                "filter_size",
                "num_results",
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
    pytest.csv_log_path_hnsw = csv_log_path_hnsw  # type: ignore
    yield

    # Teardown code
    pass  # noqa: PIE790


@pytest.fixture(scope="module")
def hnsw() -> tuple[HnswIndex, Metadata]:
    settings = Settings()  # type: ignore # uses the environment variables

    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    hnsw = HnswIndex(
        path=settings.hnsw_index_path, metadata=metadata
    )
    return hnsw, metadata




