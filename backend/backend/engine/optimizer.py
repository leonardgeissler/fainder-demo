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


class MergeKeywords(Transformer[Token, ParseTree]):
    """
    This transformer merges lucene queries into a single query string.
    And optionally merges keyword terms into a single keyword term.
    When on the same level.
    """

    # TODO: add support for merging terms when not all terms are keyword terms
    # so merging: "kw(a) AND kw(b) AND col(..)" -> "kw(a AND b) AND col(..)"

    def _merge_terms(
        self, items: list[ParseTree | Token], operator: str, rule_name: str
    ) -> ParseTree:
        # check all items are keyword terms
        all_keyword_terms = check_all_keyword_terms(items)

        # if they are, merge them into a single keyword term and return it
        if all_keyword_terms and _are_parse_trees(items):
            return Tree(
                "keyword_op",
                [
                    Token(
                        "STRING", f" {operator} ".join(get_keyword_string(tree) for tree in items)
                    )
                ],
            )

        # otherwise, return the original tree
        return Tree(Token("RULE", rule_name), items)

    def conjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        logger.trace(f"Conjunction items: {items}")
        return self._merge_terms(items, "AND", "conjunction")

    def disjunction(self, items: list[ParseTree | Token]) -> ParseTree:
        return self._merge_terms(items, "OR", "disjunction")

    def query(self, items: list[ParseTree | Token]) -> ParseTree:
        return Tree(Token("RULE", "query"), items)
