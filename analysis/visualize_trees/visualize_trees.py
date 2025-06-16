# requires pydot and graphviz to be installed in the environment
# and sudo apt install graphviz

from backend.engine.parser import Parser
from backend.engine.optimizer import Optimizer
from lark import Token, tree, Transformer, ParseTree
import os

queries = {
    "example_query_excecution": 'kw("cancer") AND col(name("age"; 0) AND (pp(1.0;gt;50) OR pp(1.0;lt;30)))',
    "complex_query_for_keyword_merging": 'kw("cancer") AND col(name("age"; 0) AND pp(1.0;gt;50)) AND (kw("diabetes") OR kw("heart disease"))',
    "query_justifying_column_node_rule": 'col(name("age"; 0)) AND col(pp(1.0;gt;50))',
    "query_example_for_read_write_groups": "kw('cancer') AND NOT col(NOT name('age'; 0) AND (NOT pp(1.0;gt;50) OR pp(1.0;lt;30)))",
    "query_justifying_junction_spliting": 'col(pp(1.0;ge;100) AND pp(1.0;le;101) AND pp(0.1;ge;1))'
}


class DeleteLeafNodes(Transformer):
    """
    Transformer to delete leaf nodes from the parse tree.
    """

    def keyword_op(self, items: list[Token]):
        """
        Deletes keyword operation nodes.
        """
        return ParseTree("keyword_op", [])

    def name_op(self, items: list[Token]):
        """
        Deletes name nodes.
        """
        return ParseTree("name_op", [])

    def percentile_op(self, items: list[Token]):
        """
        Deletes percentile operation nodes.
        """
        return ParseTree("percentile_op", [])
    
class MergeTokens(Transformer):
    """
    Transformer to merge tokens in the parse tree.
    """

    def keyword_op(self, items: list[Token]):
        """
        Merges keyword operation nodes.
        """
        t = Token("Value", " ; ".join(item.value for item in items))
        return ParseTree("keyword_op", [t])

    def name_op(self, items: list[Token]):
        """
        Merges name nodes.
        """
        t = Token("Value", " ; ".join(item.value for item in items))
        return ParseTree("name_op", [t])

    def percentile_op(self, items: list[Token]):
        """
        Merges percentile operation nodes.
        """
        t = Token("Value", " ; ".join(item.value for item in items))
        return ParseTree("percentile_op", [t])


folder = "analysis/visualize_trees/trees"
if not os.path.exists(folder):
    os.makedirs(folder)

folder2 = "analysis/visualize_trees/trees_editable"


def visualize_trees():
    """
    Visualizes the parse trees for the given queries.
    """
    parser = Parser()
    optimizer = Optimizer()
    delete_leaf_nodes = DeleteLeafNodes()
    merge_tokens = MergeTokens()
    for query_name, query in queries.items():
        print(f"Visualizing parse tree for query: {query_name}")
        parsered_tree = parser.parse(query)
        print(f"Parse tree for query: {query}")
        # save the tree to a file
        tree.pydot__tree_to_png(
            delete_leaf_nodes.transform(parsered_tree),
            f"{folder}/parse_tree_{query_name}.png",
            rankdir="TB",
        )
        tree.pydot__tree_to_png(
            merge_tokens.transform(parsered_tree),
            f"{folder}/parse_tree_{query_name}_with_leaves.png",
            rankdir="TB",
        )
        tree.pydot__tree_to_dot(
            delete_leaf_nodes.transform(parsered_tree),
            f"{folder2}/parse_tree_{query_name}.dot",
            rankdir="TB",
        )
        tree.pydot__tree_to_dot(
            merge_tokens.transform(parsered_tree),
            f"{folder2}/parse_tree_{query_name}_with_leaves.dot",
            rankdir="TB",
        )
        optimized_tree = optimizer.optimize(parsered_tree)
        print(f"Optimized parse tree for query: {query_name}")
        # save the optimized tree to a file
        tree.pydot__tree_to_png(
            delete_leaf_nodes.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}.png",
            rankdir="TB",
        )
        tree.pydot__tree_to_png(
            merge_tokens.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}_with_leaves.png",
            rankdir="TB",
        )
        tree.pydot__tree_to_dot(
            delete_leaf_nodes.transform(optimized_tree),
            f"{folder2}/optimized_parse_tree_{query_name}.dot",
            rankdir="TB",
        )
        tree.pydot__tree_to_dot(
            merge_tokens.transform(optimized_tree),
            f"{folder2}/optimized_parse_tree_{query_name}_with_leaves.dot",
            rankdir="TB",
        )


if __name__ == "__main__":
    visualize_trees()
    print("Parse trees have been visualized and saved as PNG files.")
