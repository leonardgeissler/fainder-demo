# Keyword Merging Tests

This directory contains tests and benchmarks for keyword merging optimization functionality. The tests validate that keyword merging produces correct results while improving performance.

## Files

- `keyword_merging_test.py` - Main test suite for keyword merging functionality
- `generate_keyword_test_cases.py` - Script to generate test cases for keyword merging
- `analysis.ipynb` - Jupyter notebook for analyzing keyword merging performance and correctness
- `conftest.py` - Pytest configuration and fixtures

## Test Methodology

The tests compare:
- **Merged Queries**: Queries with keyword merging optimization enabled
- **Unmerged Queries**: Equivalent queries without keyword merging
- **Result Consistency**: Ensuring both approaches return identical results
- **Performance Impact**: Measuring execution time differences

## Key Functions

- `execute_and_time()` - Executes queries and measures execution time
- `run_comparison()` - Compares merged vs unmerged query performance
- `generate_keyword_test_cases()` - Creates diverse test scenarios

## Test Cases

The test suite includes:
- Simple keyword combinations
- Complex multi-keyword queries
- Edge cases with special characters
- Performance stress tests with many keywords
- Correctness validation across different scenarios

## Running the Tests

```bash
# Run all keyword merging tests
pytest analysis/keyword_merging_tests/

```

## Analysis

The `analysis.ipynb` notebook provides:
- Performance comparison charts between merged and unmerged queries
- Correctness validation summaries
- Statistical analysis of performance improvements
- Identification of optimal keyword merging scenarios

## Metrics Tracked

- **Execution Time**: Time difference between merged and unmerged queries
- **Result Consistency**: Boolean flag indicating if results match
- **Term Count**: Number of terms in the query
- **Performance Improvement**: Percentage improvement with keyword merging

## Expected Outcomes

Keyword merging should:
- Maintain 100% result consistency
- Improve query execution time
- Reduce computational overhead
- Scale well with increasing keyword counts

## Debugging

If tests fail:
1. Check result consistency first
2. Verify query parsing is correct
3. Examine execution time patterns
4. Review optimization logic in the backend
