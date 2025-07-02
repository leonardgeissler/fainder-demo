# requires pydot and graphviz to be installed in the environment
# and sudo apt install graphviz

import cairosvg
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
        graph.write(filename, format='png')
    elif format == "dot":
        graph.write(filename)
    elif format == "svg":
        graph.write(filename, format='svg')
    else:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats are 'png', 'dot', and 'svg'."
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
            f"{folder}/parse_tree_{query_name}.svg",
            "svg",
        )  
        create_uniform_tree_visualization(
            merge_tokens.transform(parsed_tree),
            f"{folder}/parse_tree_{query_name}_with_leaves.svg",
            "svg",
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
            f"{folder}/optimized_parse_tree_{query_name}.svg",      
            "svg",
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

    # load the edited trees from the edited_trees folder
    edited_trees = glob.glob("analysis/visualize_trees/edited_trees/*.dot")
    for edited_tree in edited_trees:
        os.system(f"dot -Tpng {edited_tree} -o {edited_tree.replace('.dot', '.png')}")
        os.system(f"dot -Tsvg {edited_tree} -o {edited_tree.replace('.dot', '.svg')}")


def create_normalized_pngs(width=300):
    from PIL import Image
    import re
    
    # Get SVG files first since we're processing those
    svg_files = glob.glob(f"{folder}/*.svg")
    svg_files += glob.glob("analysis/visualize_trees/edited_trees/*.svg")
    print(f"Found {len(svg_files)} SVG files to compile.")
    
    if not svg_files:
        print("No SVG files found to process.")
        return
    
    # Get dimensions from corresponding PNG files or calculate from SVG
    svg_dimensions = []
    max_width = 0
    
    for svg_file in svg_files:
        # Try to get dimensions from corresponding PNG file first
        png_file = svg_file.replace('.svg', '.png')
        if os.path.exists(png_file):
            img = Image.open(png_file)
            width_val, height_val = img.size
        else:
            # If PNG doesn't exist, try to parse SVG dimensions
            with open(svg_file, 'r') as f:
                content = f.read()
            # Extract width and height from SVG
            width_match = re.search(r'width="(\d+(?:\.\d+)?)', content)
            height_match = re.search(r'height="(\d+(?:\.\d+)?)', content)
            if width_match and height_match:
                width_val = float(width_match.group(1))
                height_val = float(height_match.group(1))
            else:
                # Default fallback dimensions
                width_val, height_val = 800, 600
        
        svg_dimensions.append((width_val, height_val))
        max_width = max(max_width, width_val)
    
    scale_factor = width / max_width if max_width > width else 1.0
    print(f"Scaling factor: {scale_factor}")
    
    for i, svg_file in enumerate(svg_files):
        with open(svg_file, "r") as f:
            content = f.read()
        
        # Calculate scaled dimensions
        orig_width, orig_height = svg_dimensions[i]
        scaled_height = max(1, round(orig_height * scale_factor))
        scaled_width = max(1, round(orig_width * scale_factor))
        
        # Update SVG content with new dimensions
        content = re.sub(r'width="\d+(\.\d+)?(px|%|pt)?"', f'width="{scaled_width}px"', content)
        content = re.sub(r'height="\d+(\.\d+)?(px|%|pt)?"', f'height="{scaled_height}px"', content)

        # Remove scale(..) transformation if present
        content = re.sub(r'scale\(\s*[^)]+\s*\)', '', content)

        print(f"Processing {svg_file}: {orig_width}x{orig_height} -> {scaled_width}x{scaled_height}")
        
        # Save the modified SVG content back to the file
        with open(svg_file, "w") as f:
            f.write(content)

        # Convert SVG to PNG
        try:
            cairosvg.svg2png(
                url=svg_file,
                write_to=svg_file.replace('.svg', '_small.png'),
            )
        except Exception as e:
            print(f"Error converting {svg_file} to PNG: {e}")


if __name__ == "__main__":
    visualize_trees()
    create_normalized_pngs()
    # format_trees()
    print("Parse trees have been visualized and saved as PNG files.")
