# Fainder Results Size Analysis

This directory contains tools for analyzing result set sizes under different query conditions and thresholds. This is the basis for the cost estimation for estimating size of a query result.

## Files

- `run.py` - Main script for running result size analysis
- `analysis.ipynb` - Jupyter notebook for visualizing and analyzing result size patterns
- `figures/` - Directory containing generated plots and visualizations

## Analysis Parameters

The analysis covers:
- **Thresholds**: `[1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000]`
- **Percentiles**: `[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]`
- **Comparisons**: `["le"]` - Less than/equal operations
- **Fainder Modes**: `[FULL_RECALL, EXACT]` - Different precision modes

## Running the Analysis

```bash
# Run the complete result size analysis
python -m analysis.fainder_results_size.run

```

## Output

The analysis generates:
- CSV files with detailed result size data
- Performance metrics for different parameter combinations

## Visualization

Use the `analysis.ipynb` notebook to:
- Calculate the OLS Regression Results for a Dataset
- Plot result size distributions across different thresholds

## Key Insights

This analysis helps answer:
- How does result size scale with different threshold values?
- What's the impact of percentile selection on result count?
- How do different Fainder modes affect result set sizes?
- Are there optimal parameter combinations for specific use cases?

## Use Cases

**This analysis is useful for:**
- Query optimization and cost estimation
