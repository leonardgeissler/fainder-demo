# Performance Testing Framework

This directory contains a framework for running performance tests using Hydra for configuration.

## Directory Structure

- `conf/`: Configuration files for Hydra
  - `config.yaml`: Main configuration file
  - `engines/`: Engine configurations
  - `experiment/`: Experiment configurations
  - `keywords/`: Keyword configurations
  - `percentiles/`: Percentile configurations
  - `query_generation/`: Query generation configurations
- `test_cases/`: Generated test cases (JSON files)

## Running Tests

You can run the performance tests using the following command:

```bash
# Run with default configuration
python -m analysis.performance_tests.run

# Run with specific experiment configuration
python -m analysis.performance_tests.run experiment=custom_experiment

# Run with multiple configuration overrides
python -m analysis.performance_tests.run experiment=custom_experiment keywords=custom_keywords

# Run with multiple configurations after each other
python -m analysis.performance_tests.run -m experiment=custom_experiment,other_experiment
```

## Configuration

The configuration is managed using Hydra and dataclasses. The main configuration is defined in `config_models.py` as a set of dataclasses.

To generate the JSON schema for the configuration models:

```bash
uv run analysis.performance_tests.generate_schema
```

## Adding New Test Cases

To add new test cases, you can:

1. Modify the existing configuration files in `conf/`
2. Create new test case generation functions in `generate_eval_test_cases.py`
3. Update the list of enabled tests in `conf/experiment/default.yaml`

## Results

Test results are stored in:
- `logs/performance/git_heads/{git_hash}/all/`: All performance results
- `logs/performance/git_heads/{git_hash}/{test_name}/`: Test-specific results
- `logs/profiling/`: Profiling results (if enabled)

## Analysis

The framework includes several Jupyter notebooks for analyzing performance test results:

### Core Analysis Notebooks

#### `analysis.ipynb`
Main analysis notebook for exploring performance test results:
- Loads the latest performance test data from CSV files
- Provides visualization of execution times across different scenarios
- Compares performance between different Fainder modes
- Analyzes query complexity vs execution time relationships
- Generates performance summary statistics

#### `regression_analysis.ipynb`
Dedicated notebook for regression testing and performance comparison:
- Compares performance between two different commits/versions
- Identifies performance regressions and improvements
- Provides statistical analysis of performance changes
- Generates before/after comparison charts
- Helps validate that optimizations don't introduce performance regressions

#### `analysis_with_num_workers.ipynb`
Specialized analysis focusing on multi-threading performance:
- Analyzes performance scaling with different numbers of worker threads
- Compares single-threaded vs multi-threaded execution
- Identifies optimal worker count configurations
- Measures parallelization efficiency

#### `comparions_analysis_multiple_sources.ipynb`
Advanced notebooks for comparing performance across multiple data sources:
- Cross-dataset performance analysis
- Multi-source regression testing
- Comparative performance benchmarking

### Generated Outputs

Analysis results are saved in:
- `figures/`: Generated plots and visualizations
- Performance summary reports
- Statistical analysis results
- Comparison charts between different configurations
