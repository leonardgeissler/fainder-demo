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
    return tree.children[0].value


def _are_parse_trees(items: list[Any]) -> TypeGuard[list[ParseTree]]:
    return all(isinstance(item, Tree) for item in items)


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
            # Include pending negation only if there are other keywords to merge with
            if pending_negation:
                current_keywords.insert(0, pending_negation)
                pending_negation = None
            merged = Tree(
                "keyword_op",
                [
                    Token(
                        "STRING",
                        f" {operator} ".join(get_keyword_string(kw) for kw in current_keywords),
                    )
                ],
            )
            result.append(merged)
            current_keywords.clear()
        elif pending_negation:
            # If we have a pending negation but no keywords to merge with, add it as-is
            result.append(pending_negation)
            pending_negation = None

    for item in items:
        if item.data == "keyword_op":
            current_keywords.append(item)
        elif (
            item.data == "negation"
            and isinstance(item.children[0], Tree)
            and item.children[0].data == "keyword_op"
        ):
            if current_keywords:
                # If we already have keywords, merge the negation with them
                current_keywords.append(item)
            else:
                # Otherwise, hold it pending to see if more keywords follow
                flush_keywords()
                pending_negation = item
        else:
            flush_keywords()
            result.append(item)

    flush_keywords()
    return result


class MergeKeywords(Transformer[Token, ParseTree]):
    """
    This transformer merges lucene queries into a single query string.
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
        return Tree(Token("RULE", rule_name), merged_items)  # type: ignore

    def conjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        logger.trace(f"Conjunction items: {items}")
        return self._merge_terms(items, "AND", "conjunction")

    def disjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        return self._merge_terms(items, "OR", "disjunction")

    def query(self, items: list[ParseTree | Token]) -> ParseTree:
        return Tree(Token("RULE", "query"), items)
