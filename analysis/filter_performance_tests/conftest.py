import csv
import json
import sys
from collections.abc import Generator
from pathlib import Path
import time
from typing import Any

import pytest
from loguru import logger

from backend.column_index import ColumnIndex
from backend.config import Metadata, Settings
from backend.fainder_index import FainderIndex
from backend.lucene_connector import LuceneConnector
from backend.query_evaluator import QueryEvaluator


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

    performance_log_dir_lucene = Path("logs/performance_filters_lucene")
    performance_log_dir_lucene.mkdir(exist_ok=True)

    performance_log_dir_hnsw = Path("logs/performance_filters_hnsw")
    performance_log_dir_hnsw.mkdir(exist_ok=True)

    # Create CSV performance log file and write headers
    csv_log_path_fainder = (
        performance_log_dir_fainder / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
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
                "additional_filter_size",
                "filter_size",
                "num_results",
            ]
        )

    csv_log_path_lucene = (
        performance_log_dir_lucene / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )

    with csv_log_path_lucene.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "keyword",
                "execution_time",
                "additional_filter_size",
                "filter_size",
                "num_results",
            ]
        )

    csv_log_path_hnsw = (
        performance_log_dir_hnsw / f"performance_metrics_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    )

    with csv_log_path_hnsw.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            [
                "timestamp",
                "column_name",
                "k",
                "execution_time",
                "additional_filter_size",
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
        "logs/test_{time:YYYY-MM-DD HH:mm:ss}.log",
        format="{time:HH:mm:ss} | {level: >5} | {file}:{line} | {message}",
        level="DEBUG",
    )
    # Add CSV log path to pytest's config
    pytest.csv_log_path = csv_log_path_fainder  # type: ignore
    pytest.csv_log_path_lucene = csv_log_path_lucene # type: ignore
    pytest.csv_log_path_hnsw = csv_log_path_hnsw # type: ignore
    yield

    # Teardown code
    pass  # noqa: PIE790


@pytest.fixture(scope="module")
def evaluator() -> QueryEvaluator:
    settings = Settings() # type: ignore # uses the environment variables 
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    lucene_connector = LuceneConnector(settings.lucene_host, settings.lucene_port)
    fainder_index = FainderIndex(
        metadata=metadata,
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
    )
    column_index = ColumnIndex(
        path=settings.hnsw_index_path, metadata=metadata, use_embeddings=False
    )
    return QueryEvaluator(
        lucene_connector=lucene_connector,
        fainder_index=fainder_index,
        hnsw_index=column_index,
        metadata=metadata,
        cache_size=-1,
    )
