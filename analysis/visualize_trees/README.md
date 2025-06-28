# Visualize Trees

This directory contains tools for visualizing query parse trees and optimization transformations. The visualization helps understand how queries are parsed, optimized, and executed.

## Files

- `visualize_trees.py` - Main script for generating tree visualizations
- `trees/` - Directory containing generated tree visualizations
- `trees_editable/` - Directory with editable tree formats
- `edited_trees/` - Directory with manually edited tree visualizations

## Dependencies

This module requires additional dependencies:
```bash
# Python packages
pip install pydot graphviz

# System dependency (Ubuntu/Debian)
sudo apt install graphviz
```

## Query Examples

The script includes several predefined queries for visualization:
- `example_query_excecution` - Basic query with keywords and column operations
- `complex_query_for_keyword_merging` - Complex query demonstrating keyword merging
- `query_justifying_column_node_rule` - Query showing column node optimization
- `query_example_for_read_write_groups` - Query with NOT operations
- `query_justifying_junction_spliting` - Query demonstrating junction splitting

## Tree Transformations

The visualization includes several transformer classes:
- `DeleteLeafNodes` - Removes leaf nodes for simplified view
- Custom transformers for different optimization stages

## Usage

```bash
# Generate visualizations for all predefined queries
python analysis/visualize_trees/visualize_trees.py

# The script will generate DOT files and PNG images in the respective directories
```

## Output Formats

The tool generates:
- **DOT files**: GraphViz source files for further editing
- **PNG images**: Rendered tree visualizations
- **Editable formats**: Intermediate formats for manual editing

## Directory Structure

- `trees/` - Final rendered tree images
- `trees_editable/` - DOT source files that can be modified
- `edited_trees/` - Manually edited versions of trees

## Use Cases

Tree visualization is useful for:
- **Query Analysis**: Understanding how complex queries are parsed
- **Optimization Debugging**: Seeing the effects of optimization passes
- **Documentation**: Creating visual documentation of query structures
- **Education**: Teaching query parsing and optimization concepts
- **Research**: Analyzing query complexity and transformation patterns

## Customization

To add new queries:
1. Add the query string to the `queries` dictionary in `visualize_trees.py`
2. Run the script to generate visualizations
3. Manually edit the generated DOT files if needed
4. Re-render with GraphViz for final images

## Technical Notes

- The parser uses Lark for query parsing
- GraphViz handles the actual tree rendering
- Custom transformers can be added for specific visualization needs
- The visualization supports both pre and post-optimization trees
