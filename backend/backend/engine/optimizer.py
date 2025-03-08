from typing import Any, TypeGuard

from lark import ParseTree, Token, Transformer, Tree, Visitor
from loguru import logger


class ParentAnnotator(Visitor[Token]):
    def __default__(self, tree: ParseTree) -> ParseTree:
        for subtree in tree.children:
            if isinstance(subtree, Tree):
                assert not hasattr(subtree, "parent")
                subtree.parent = tree.data  # type: ignore

        return tree


LEAF_COSTS = {
    "keyword_op": 1,
    "percentile_op": 2,
    "name_op": 1,
}

EXTRA_COSTS = {
    "col_op": 1,  # extra cost for column
    "negation": 0,  # extra cost for negation
}


class CostSorter(Visitor[Token]):
    """
    This visitor annotates each node with a cost value as a tree attribute.
    It should only be used bottom-up so using .visit().
    It also sorts the children of each node by cost.
    """

    def __default__(self, tree: ParseTree | Token) -> ParseTree | Token:
        if isinstance(tree, Token):
            return tree

        if tree.data in LEAF_COSTS:
            # If the node is a leaf node, set its cost
            tree.cost = LEAF_COSTS[tree.data]  # type: ignore
            return tree

        # Calculate costs from children
        total_cost = 0
        for child in tree.children:
            if isinstance(child, Tree):
                # Get cost from child if it exists
                total_cost += getattr(child, "cost", 0)

        # Sort children by cost
        logger.trace(f"Before sorting: {tree.children}")
        tree.children.sort(key=lambda x: getattr(x, "cost", 0))
        logger.trace(f"After sorting: {tree.children}")

        # Add any additional cost for the current node
        if tree.data in EXTRA_COSTS:
            total_cost += EXTRA_COSTS[tree.data]

        # Store the cost on the tree node
        tree.cost = total_cost  # type: ignore

        return tree


def check_all_keyword_terms(items: list[ParseTree | Token]) -> bool:
    if not _are_parse_trees(items):
        return False
    for tree in items:
        if tree.data == "keyword_op":
            continue
        if tree.data == "negation":
            subtree = tree.children[0]
            if not isinstance(subtree, Tree) or subtree.data != "keyword_op":
                return False
        else:
            return False
    return True


def get_keyword_string(tree: ParseTree) -> str:
    if tree.data == "negation":
        subtree = tree.children[0]
        assert isinstance(subtree, Tree)
        return "-: (" + get_keyword_string(subtree) + ")"
    assert tree.data == "keyword_op"
    assert len(tree.children) == 1
    assert isinstance(tree.children[0], Token)
    return str(tree.children[0].value)[1:-1]


def _are_parse_trees(items: list[Any]) -> TypeGuard[list[ParseTree]]:
    return all(isinstance(item, Tree) for item in items)


def _create_merged_keyword(keyword_strings: list[str], operator: str) -> ParseTree:
    """Creates a merged keyword ParseTree from a list of keyword strings."""
    if len(keyword_strings) == 1:
        merged_query = f"'({keyword_strings[0]})'"
    else:
        first_keyword = keyword_strings[0]
        rest_keywords = [f"{operator} {kw}" for kw in keyword_strings[1:]]
        merged_parts = [first_keyword, *rest_keywords]
        merged_query = f"'({' '.join(merged_parts)})'"

    return Tree("keyword_op", [Token("STRING", merged_query)])


def _is_keyword_or_negated_keyword(item: ParseTree) -> bool:
    """Check if a ParseTree is a keyword or negated keyword."""
    if item.data == "keyword_op":
        return True
    return (
        item.data == "negation"
        and isinstance(item.children[0], Tree)
        and item.children[0].data == "keyword_op"
    )


def merge_consecutive_keywords(items: list[ParseTree], operator: str) -> list[ParseTree]:
    """Merges consecutive keyword terms in a list of parse trees."""
    if len(items) <= 1:
        return items

    result: list[ParseTree] = []
    current_keywords: list[ParseTree] = []
    pending_negation: ParseTree | None = None

    def flush_keywords() -> None:
        nonlocal pending_negation
        if current_keywords:
            if pending_negation:
                current_keywords.insert(0, pending_negation)
                pending_negation = None

            keyword_strings = [get_keyword_string(kw) for kw in current_keywords]
            merged = _create_merged_keyword(keyword_strings, operator)
            result.append(merged)
            current_keywords.clear()
        elif pending_negation:
            result.append(pending_negation)
            pending_negation = None

    for item in items:
        if item.data == "keyword_op":
            current_keywords.append(item)
        elif _is_keyword_or_negated_keyword(item):
            if current_keywords:
                current_keywords.append(item)
            else:
                flush_keywords()
                pending_negation = item
        else:
            flush_keywords()
            result.append(item)

    flush_keywords()
    return result


class MergeKeywords(Transformer[Token, ParseTree]):
    """
    This transformer merges keyword queries into a single query string.
    And optionally merges keyword terms into a single keyword term.
    When on the same level.
    """

    def _merge_terms(
        self, items: list[ParseTree | Token], operator: str, rule_name: str
    ) -> ParseTree:
        if not _are_parse_trees(items):
            return Tree(Token("RULE", rule_name), items)

        # Merge consecutive keyword terms
        merged_items = merge_consecutive_keywords(items, operator)
        if len(merged_items) == 1:
            return merged_items[0]
        return Tree(Token("RULE", rule_name), merged_items)  # type: ignore

    def conjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        logger.trace(f"Conjunction items: {items}")
        return self._merge_terms(items, "AND", "conjunction")

    def disjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        return self._merge_terms(items, "OR", "disjunction")

    def query(self, items: list[ParseTree | Token]) -> ParseTree:
        return Tree(Token("RULE", "query"), items)
