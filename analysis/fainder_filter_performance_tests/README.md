# Fainder Filter Performance Tests

This directory contains performance benchmarks specifically focused on Fainder's filtering capabilities. The tests measure execution time and resource usage under various filtering scenarios.

## Files

- `fainder_filter_performance_test.py` - Main performance test suite for filter operations
- `analysis.ipynb` - Jupyter notebook for performance analysis and visualization

## Test Parameters

The performance tests evaluate:
- **References**: `[1, 100, 10000, 1000000, 10000000]` - Different reference values
- **Comparisons**: `["le" ,"ge"]` - Less than/equal operations
- **Percentiles**: `[0.1, 0.5, 0.9]` - Different percentile thresholds
- **Fainder Modes**: `[FULL_RECALL, EXACT]` - Different precision modes
- **Filter Sizes**: Various filter sizes to test scalability

## Filter Size Categories

- **Right Filter Sizes**: `[100, 10000, 100000, 1000000, 10000000, 30000000, 50000000]`
- **Wrong Filter Sizes**: `[0, 1000, 10000, 100000, 1000000, 10000000, 30000000, 50000000]`

## Running the Tests

```bash
# Run all performance tests
uv run analysis/fainder_filter_performance_tests/fainder_filter_performance_test.py

```

## Analysis

The `analysis.ipynb` notebook provides:
- Performance trend analysis across different parameters
- Comparison between different Fainder modes
- Scalability analysis with increasing filter sizes
- Bottleneck identification and optimization recommendations

## Metrics Collected

- **Execution Time**: Time taken for filter operations

## Use Cases

These tests are essential for:
- Optimization validation
