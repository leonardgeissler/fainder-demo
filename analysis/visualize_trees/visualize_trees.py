# requires pydot and graphviz to be installed in the environment
# and sudo apt install graphviz

from backend.engine.parser import Parser
from backend.engine.optimizer import Optimizer
from lark import tree
import os
queries = {
    "example_query_excecution":'kw("cancer") AND col(name("age"; 0) AND (pp(1.0;gt;50) OR pp(1.0;lt;30)))',
    "complex query for keyword merging": 'kw("cancer") AND col(name("age"; 0) AND pp(1.0;gt;50)) AND (kw("diabetes") OR kw("heart disease"))',
    "query_justifying_column_node_rule": 'col(name("age"; 0)) AND col(pp(1.0;gt;50))',
    "query_example_for_read_write_groups": "kw('cancer') AND NOT col(NOT name('age'; 0) AND (NOT pp(1.0;gt;50) OR pp(1.0;lt;30)))",
    "query_justifying_junction_spliting": 'kw("cancer") AND col(pp(0.5;ge;100) AND NOT pp(0.5;ge;100)) AND col(NOT pp(0.9;ge;100) AND NOT pp(0.5;ge;1000000) AND NOT pp(0.9;le;1000000))',

    

}
folder = "analysis/visualize_trees/trees"
if not os.path.exists(folder):
    os.makedirs(folder)

def visualize_trees():
    """
    Visualizes the parse trees for the given queries.
    """
    parser = Parser()
    optimizer = Optimizer()
    for query_name, query in queries.items():
        print(f"Visualizing parse tree for query: {query_name}")
        parsered_tree = parser.parse(query)
        print(f"Parse tree for query: {query}")
        # save the tree to a file
        tree.pydot__tree_to_png(parsered_tree, f"{folder}/parse_tree_{query_name}.png", rankdir="TB")
        optimized_tree = optimizer.optimize(parsered_tree)
        print(f"Optimized parse tree for query: {query_name}")
        # save the optimized tree to a file
        tree.pydot__tree_to_png(optimized_tree, f"{folder}/optimized_parse_tree_{query_name}.png", rankdir="TB")

if __name__ == "__main__":
    visualize_trees()
    print("Parse trees have been visualized and saved as PNG files.")