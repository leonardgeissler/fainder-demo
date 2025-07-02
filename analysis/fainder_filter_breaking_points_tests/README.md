# Fainder Filter Breaking Points Tests

This directory contains tests designed to identify breaking points and edge cases in Fainder's filter functionality. The tests explore various scenarios where the filtering system might fail or produce unexpected results.

## Files

- `fainder_filter_breaking_points_test.py` - Main test suite for breaking point scenarios
- `test_number_of_results.py` - Tests focusing on result count validation
- `analysis.ipynb` - Jupyter notebook for analyzing test results and visualizing breaking points
- `conftest.py` - Pytest configuration and fixtures
- `excecution/` - Directory containing execution logs and results

## Test Parameters

The tests cover various combinations of:
- **References**: `[1, 100000, 10000000]` - Different reference values for percentile operations
- **Comparisons**: `["le", "ge"]` - Less than/equal and greater than/equal operations
- **Percentiles**: `[0.1, 0.5, 0.9]` - Different percentile thresholds
- **Fainder Modes**: `["full_precision", "exact"]` - Different precision modes
- **Keywords**: Various test keywords to stress the system

## Running the Tests

```bash
# Run the breaking point tests
pytest analysis/fainder_filter_breaking_points_tests/

```

## Analysis

Use the `analysis.ipynb` notebook to:
- Visualize test results and identify patterns in breaking points
- Analyze performance degradation under extreme conditions
- Generate reports on system limitations and edge cases

## Purpose

These tests help ensure that:
- The system gracefully handles edge cases
- Performance remains acceptable under stress conditions
- Error handling is robust across different scenarios
- System limitations are well-documented and understood
