from backend.lucene_connector import LuceneConnector
from backend.tantivy_index import TantivyIndex
import pytest
import time
from backend.config import Settings

KEYWORDS = ["lung", "lung OR cancer"]


@pytest.mark.parametrize("keyword", KEYWORDS)
def test_comparison(keyword: str):
    settings = Settings()  # type: ignore # uses the environment variables

    lucene_connector = LuceneConnector("127.0.0.1", "8001")

    tantivy_index = TantivyIndex(str(settings.tantivy_path))

    start_time = time.perf_counter()
    lucene_results, _, _ = lucene_connector.evaluate_query(keyword)
    lucene_time = time.perf_counter() - start_time

    start_time = time.perf_counter()
    tantivy_results, _, _ = tantivy_index.search(keyword)
    tantivy_time = time.perf_counter() - start_time

    assert len(lucene_results) == len(tantivy_results)

    assert lucene_results == tantivy_results
    assert lucene_time < tantivy_time
