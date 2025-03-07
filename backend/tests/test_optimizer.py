import pytest

from backend.engine.optimizer import CostSorter, MergeKeywords

from .assets.test_cases_optimizer import TEST_CASES_OPTIMIZER, CaseOpt


@pytest.mark.parametrize(
    ("test_name", "test_case"),
    [(name, case) for name, case in TEST_CASES_OPTIMIZER.items()],
)
def test_optimizer(test_name: str, test_case: CaseOpt) -> None:
    cost_sorter = CostSorter()
    merge_keywords = MergeKeywords()

    # Execute with all configurations
    result_kw_merge_without_sorting = merge_keywords.transform(test_case["input_parse_tree"])

    assert result_kw_merge_without_sorting == test_case["expected_kw_merge_without_sorting"]

    sorted_tree = cost_sorter.visit(test_case["input_parse_tree"])

    assert sorted_tree == test_case["expected_with_sorting"]

    result_kw_merge_with_sorting = merge_keywords.transform(sorted_tree)

    assert result_kw_merge_with_sorting == test_case["expected_kw_merge_with_sorting"]
