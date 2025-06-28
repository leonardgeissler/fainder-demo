import csv
import json
import sys
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

from backend.indices.keyword_op import TantivyIndex
import pytest
from loguru import logger

from backend.indices.name_op import HnswIndex as ColumnIndex
from backend.config import Metadata, Settings, ExecutorType
from backend.indices.percentile_op import FainderIndex
from backend.engine import Engine
from backend.engine import Optimizer


@pytest.fixture(autouse=True, scope="module")
def _setup_and_teardown() -> Generator[None, Any, None]:  # pyright: ignore[reportUnusedFunction]
    """
    Generic setup and teardown fixture that runs before and after each test.
    """
    # Setup code

    # Create logs directory if it doesn't exist
    base_log_dir = Path("logs")
    base_log_dir.mkdir(exist_ok=True)
    log_dir = Path("logs/logs")
    log_dir.mkdir(exist_ok=True)

    performance_log_dir = Path("logs/kw_merging_performance")
    performance_log_dir.mkdir(exist_ok=True)

    # Create CSV performance log file and write headers
    csv_log_path = (
        performance_log_dir / f"keyword_merging_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )
    with csv_log_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "query",
                "excecution_time_with_merging",
                "excecution_time_without_merging",
                "is_consistent",
                "number_of_terms",
                "number_of_results_with_merging",
                "number_of_results_without_merging",
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

    # Add CSV log path to pytest's config
    pytest.csv_log_path = csv_log_path  # type: ignore

    yield

    # Teardown code
    pass


@pytest.fixture(scope="module")
def engine() -> tuple[Engine, Engine]:
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    fainder_index = FainderIndex(
        rebinning_path=None,
        conversion_path=None,
        histogram_path=None,
    )
    column_index = ColumnIndex(
        path=settings.hnsw_index_path, metadata=metadata, use_embeddings=False
    )
    engines = (
        Engine(
            tantivy_index=TantivyIndex(
                index_path=str(settings.tantivy_path), recreate=False
            ),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.SIMPLE,
        ),
        Engine(
            tantivy_index=TantivyIndex(
                index_path=str(settings.tantivy_path), recreate=False
            ),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.SIMPLE,
        ),
    )
    engines[0].optimizer = Optimizer(keyword_merging=True)
    engines[1].optimizer = Optimizer(keyword_merging=False)
    return engines
