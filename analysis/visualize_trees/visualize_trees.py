# requires pydot and graphviz to be installed in the environment
# and sudo apt install graphviz

import glob
from backend.engine.parser import Parser
from backend.engine.optimizer import Optimizer
from lark import Token, Transformer, ParseTree, Tree
import os
import pydot

queries = {
    "example_query_excecution": 'kw("cancer") AND col(name("age"; 0) AND (pp(1.0;gt;50) OR pp(1.0;lt;30)))',
    "complex_query_for_keyword_merging": 'kw("cancer") AND col(name("age"; 0) AND pp(1.0;gt;50)) AND (kw("diabetes") OR kw("heart disease"))',
    "query_justifying_column_node_rule": 'col(name("age"; 0)) AND col(pp(1.0;gt;50))',
    "query_example_for_read_write_groups": "kw('cancer') AND NOT col(NOT name('age'; 0) AND (NOT pp(1.0;gt;50) OR pp(1.0;lt;30)))",
    "query_justifying_junction_spliting": "col(pp(1.0;ge;100) AND pp(1.0;le;101) AND pp(0.1;ge;1))",
}


class DeleteLeafNodes(Transformer):  # type: ignore
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


class MergeTokens(Transformer):  # type: ignore
    """
    Transformer to merge tokens in the parse tree.
    """

    def keyword_op(self, items: list[Token]):
        """
        Merges keyword operation nodes.
        """
        t = " \n ".join(item.value for item in items)
        return ParseTree("keyword_op", [t])

    def name_op(self, items: list[Token]):
        """
        Merges name nodes.
        """
        t = " ; ".join(item.value for item in items)
        return ParseTree("name_op", [t])

    def percentile_op(self, items: list[Token]):
        """
        Merges percentile operation nodes.
        """
        t = " ; ".join(item.value for item in items)
        return ParseTree("percentile_op", [t])


folder = "analysis/visualize_trees/trees"
if not os.path.exists(folder):
    os.makedirs(folder)


def create_uniform_tree_visualization(parse_tree, filename, format="png", rankdir="TB"):
    """
    Creates a tree visualization with uniform node sizes.
    Based on lark's pydot__tree_to_graph function with added uniform sizing.
    """
    graph = pydot.Dot(graph_type="digraph", rankdir=rankdir, dpi=100)
    # Set default node attributes for uniform sizing
    graph.set_node_defaults(
        shape="oval",
        width="1.2",
        height="0.4",
        fixedsize="true",
        fontsize="10",
        size="8,4",
    )

    i = [0]

    def new_leaf(leaf):
        width = 1.2
        if len(repr(leaf)) > 25:
            width = 4
        node = pydot.Node(
            str(i[0]),
            label=repr(leaf),
            shape="oval",
            width=width,
            height="0.4",
            fixedsize="true",
            fontsize="10",
        )
        i[0] += 1
        graph.add_node(node)
        return node

    def _to_pydot(subtree):
        color = hash(subtree.data) & 0xFFFFFF
        color |= 0x808080

        subnodes = [
            _to_pydot(child) if isinstance(child, Tree) else new_leaf(child)
            for child in subtree.children
        ]
        node = pydot.Node(
            str(i[0]),
            style="filled",
            fillcolor="#%x" % color,
            label=subtree.data,
            shape="oval",
            width="1.2",
            height="0.4",
            fixedsize="true",
            fontsize="10",
        )
        i[0] += 1
        graph.add_node(node)

        for subnode in subnodes:
            graph.add_edge(pydot.Edge(node, subnode))

        return node

    _to_pydot(parse_tree)

    if format == "png":
        graph.write_png(filename)
    elif format == "dot":
        graph.write(filename)
    else:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats are 'png' and 'dot'."
        )


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
        parsed_tree = parser.parse(query)
        print(f"Parse tree for query: {query}")
        # save the tree to a file
        create_uniform_tree_visualization(
            delete_leaf_nodes.transform(parsed_tree),
            f"{folder}/parse_tree_{query_name}.png",
            "png",
        )
        create_uniform_tree_visualization(
            merge_tokens.transform(parsed_tree),
            f"{folder}/parse_tree_{query_name}_with_leaves.png",
            "png",
        )
        create_uniform_tree_visualization(
            delete_leaf_nodes.transform(parsed_tree),
            f"{folder}/parse_tree_{query_name}.dot",
            "dot",
        )
        create_uniform_tree_visualization(
            merge_tokens.transform(parsed_tree),
            f"{folder}/parse_tree_{query_name}_with_leaves.dot",
            "dot",
        )
        optimized_tree = optimizer.optimize(parsed_tree)
        print(f"Optimized parse tree for query: {query_name}")
        # save the optimized tree to a file
        create_uniform_tree_visualization(
            delete_leaf_nodes.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}.png",
            "png",
        )
        create_uniform_tree_visualization(
            merge_tokens.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}_with_leaves.png",
            "png",
        )
        create_uniform_tree_visualization(
            delete_leaf_nodes.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}.dot",
            "dot",
        )
        create_uniform_tree_visualization(
            merge_tokens.transform(optimized_tree),
            f"{folder}/optimized_parse_tree_{query_name}_with_leaves.dot",
            "dot",
        )


def format_trees(width=300):
    # collect all pngs and scale them to a target width with the same factor for all images
    from PIL import Image

    png_files = glob.glob(f"{folder}/*.png")
    png_files += glob.glob("analysis/visualize_trees/edited_trees/*.png")
    heights = [Image.open(png).size[1] for png in png_files]
    widths = [Image.open(png).size[0] for png in png_files]
    print(f"Found {len(png_files)} PNG files with widths: {widths}")
    max_width = max(widths)
    scale_factor = width / max_width if max_width > width else 1.0
    print(f"Scaling factor: {scale_factor}")

    dot_files = glob.glob(f"{folder}/*.dot")
    dot_files += glob.glob("analysis/visualize_trees/edited_trees/*.dot")
    print(f"Found {len(dot_files)} DOT files to compile.")

    # add new size to each dot file then compile them

    for i, dot_file in enumerate(dot_files):
        with open(dot_file, "r") as f:
            content = f.read()
        # add dpi and size to the dot file
        height = int((heights[i] * scale_factor) / 100)
        width = int((widths[i] * scale_factor) / 100)

        content = content.replace('size="8,4";', f"size={width},{height};")
        with open(dot_file, "w") as f:
            f.write(content)
        print(f"Updated {dot_file} with new size and dpi.")

        os.system(f"dot -Tpng {dot_file} -o {dot_file.replace('.dot', '.png')}")


if __name__ == "__main__":
    visualize_trees()
    format_trees()
    print("Parse trees have been visualized and saved as PNG files.")
