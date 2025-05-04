# Performance Testing Framework

This directory contains a framework for running performance tests using Hydra for configuration and Pydantic for data validation.

## Directory Structure

- `conf/`: Configuration files for Hydra
  - `config.yaml`: Main configuration file
  - `engines/`: Engine configurations
  - `experiment/`: Experiment configurations
  - `keywords/`: Keyword configurations
  - `percentiles/`: Percentile configurations
  - `query_generation/`: Query generation configurations
- `schema/`: JSON schema for the Pydantic models (generated)
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
```

Alternatively, you can use the provided bash script:

```bash
./analysis/performance_tests/run_experiment.sh -c custom_experiment
```

## Configuration

The configuration is managed using Hydra and Pydantic models. The main configuration is defined in `config_models.py` as a set of Pydantic models.

To generate the JSON schema for the configuration models:

```bash
python -m analysis.performance_tests.generate_schema
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
