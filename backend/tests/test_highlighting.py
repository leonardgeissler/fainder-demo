import pytest

from backend.engine import Engine
from backend.engine.optimizer import Optimizer

from .assets.test_cases_highlighting import HIGHLIGHTING_CASES, HighlightingCase


@pytest.mark.parametrize(
    ("test_name", "test_case"), [(name, case) for name, case in HIGHLIGHTING_CASES.items()]
)
def test_highlighter(test_name: str, test_case: HighlightingCase, default_engine: Engine) -> None:
    default_engine.optimizer = Optimizer(cost_sorting=False, keyword_merging=False)
    _, highlights = default_engine.execute(test_case["query"], enable_highlighting=True)

    # Compare DocumentHighlights
    assert highlights[0] == test_case["expected"][0]
    # Compare ColumnHighlights
    assert highlights[1].all() == test_case["expected"][1].all()

