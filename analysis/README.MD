# Analysis Directory

This directory contains various analysis tools and test suites for the Fainder search engine project. Each subdirectory focuses on different aspects of performance testing, benchmarking, and visualization.

## Directory Overview

### [fainder_filter_breaking_points_tests/](./fainder_filter_breaking_points_tests/)
Tests to identify breaking points and edge cases in Fainder's filter functionality.

### [fainder_filter_performance_tests/](./fainder_filter_performance_tests/)
Performance benchmarks specifically focused on Fainder's filtering capabilities.

### [fainder_results_size/](./fainder_results_size/)
Analysis of result set sizes under different query conditions and thresholds.

### [keyword_merging_tests/](./keyword_merging_tests/)
Tests and benchmarks for keyword merging optimization functionality.

### [performance_tests/](./performance_tests/)
Comprehensive performance testing framework with Hydra configuration support. Includes specialized analysis notebooks for optimizer evaluation, regression testing, and multi-threading performance analysis.

### [visualize_trees/](./visualize_trees/)
Tools for visualizing query parse trees and optimization transformations.

## Getting Started

Each directory contains its own README with specific instructions. Most test suites can be run independently and include both programmatic tests and Jupyter notebooks for analysis.

## Common Requirements

All analysis tools require:
- Python 3.8+
- Backend dependencies (see `backend/pyproject.toml`) with development dependencies installed
- Additional dependencies as specified in individual directories
