import sys
from collections.abc import Generator
from typing import Any

import pytest
from loguru import logger

# NOTE: Not necessary yet but might be useful in the future
# @pytest.fixture(scope="session")
# def lucene_connector() -> Generator[LuceneConnector, Any, None]:
#     """
#     Fixture to create a LuceneConnector instance that can be used in tests.
#     The scope is set to 'session' to ensure the connector is created once per test session.
#     """
#     connector = LuceneConnector(
#         os.getenv("LUCENE_HOST", "127.0.0.1"), os.getenv("LUCENE_PORT", "8001")
#     )
#     yield connector
#     del connector


@pytest.fixture(autouse=True)
def _setup_and_teardown() -> Generator[None, Any, None]:
    """
    Generic setup and teardown fixture that runs before and after each test.
    """
    # Setup code
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    yield

    # Teardown code
    pass
