from collections import defaultdict
from collections.abc import Sequence

from lark import Lark, Token, Transformer, Tree, Visitor
from loguru import logger

from backend.fainder_index import FainderIndex
from backend.lucene_connector import LuceneConnector

GRAMMAR = """
    start: query
    query: expression (OPERATOR query)?
    expression: not_expr | term | "(" query ")"
    not_expr: "NOT" term | "NOT" "(" query ")"
    term: PERCENTILE_OPERATOR "(" percentileterm ")" | KEYWORD_OPERATOR "(" keywordterm ")"
    percentileterm: NUMBER ";" COMPARISON ";" NUMBER ";" IDENTIFIER
        | NUMBER ";" COMPARISON ";" NUMBER
    keywordterm: KEYWORD
    OPERATOR: "AND" | "OR" | "XOR"
    COMPARISON: "ge" | "gt" | "le" | "lt"
    PERCENTILE_OPERATOR: ("pp"i | "percentile"i) " "*
    KEYWORD_OPERATOR: ("kw"i | "keyword"i) " "*
    NUMBER: /[0-9]+(\\.[0-9]+)?/
    IDENTIFIER: /[a-zA-Z0-9_]+/
    KEYWORD: /[^;)]+/
    %ignore " "
"""


class QueryEvaluator:
    def __init__(
        self,
        doc_ids: set[int],
        lucene_connector: LuceneConnector,
        rebinning_index: FainderIndex,
        conversion_index: FainderIndex,
    ):
        self.doc_ids = doc_ids
        self.lucene_connector = lucene_connector
        self.rebinning_index = rebinning_index
        self.conversion_index = conversion_index  # NOTE: Currently unused
        self.grammar = Lark(GRAMMAR, start="start")
        self.annotator = QueryAnnotator()
        self.executor = QueryExecutor(self.doc_ids, self.lucene_connector, self.rebinning_index)

    def parse(self, query: str) -> Tree:
        return self.grammar.parse(query)

    async def execute(self, query: str, enable_filtering: bool = True) -> list[int]:
        self.annotator.reset()
        self.executor.reset()
        self.executor.enable_filtering = enable_filtering

        parse_tree = self.parse(query)
        self.annotator.visit(parse_tree)
        logger.trace(f"Parse tree: {parse_tree.pretty()}")

        result: list[int] = list(self.executor.transform(parse_tree))
        # Sort the results by score (descending), append to the end if no score is available
        result.sort(key=lambda x: self.executor.scores.get(x, -1), reverse=True)
        return result


class QueryAnnotator(Visitor):
    """
    This visitor goes top-down through the parse tree and annotates each percentile and keyword
    term with its parent operator and side (i.e., evaluation order) in the parent expression.
    """

    # TODO: We need to investigate this class because nodes are annotated too often

    def __init__(self) -> None:
        self.current_operator: str | None = None
        self.current_side: str | None = None

    def reset(self) -> None:
        self.current_operator = None
        self.current_side = None

    def query(self, tree: Tree):
        if len(tree.children) == 3:  # Has operator
            old_operator = self.current_operator
            old_side = self.current_side

            assert isinstance(tree.children[1], Token)
            self.current_operator = tree.children[1].value

            # Visit left side
            self.current_side = "left"
            self.visit(tree.children[0])

            # Visit right side
            self.current_side = "right"
            self.visit(tree.children[2])

            self.current_operator = old_operator
            self.current_side = old_side
        else:
            self.visit(tree.children[0])

    def percentileterm(self, tree: Tree) -> None:
        if self.current_operator:
            tree.children.append(self.current_operator)
            tree.children.append(self.current_side)

    def keywordterm(self, tree: Tree) -> None:
        if self.current_operator:
            tree.children.append(self.current_operator)
            tree.children.append(self.current_side)


class QueryExecutor(Transformer):
    """This transformer evaluates the parse tree bottom-up and compute the query result."""

    scores: dict[int, float]
    last_result: set[int] | None
    current_side: str | None

    def __init__(
        self,
        doc_ids: set[int],
        lucene_connector: LuceneConnector,
        fainder_index: FainderIndex,
        enable_filtering: bool = True,
    ):
        self.doc_ids: set[int] = doc_ids
        self.fainder_index = fainder_index
        self.lucene_connector = lucene_connector
        self.enable_filtering = enable_filtering
        self.reset()

    def _get_doc_filter(self, operator: str | None, side: str | None) -> set[int] | None:
        """Create a document filter for AND operators based on previous results."""
        if (
            not self.enable_filtering
            or not self.last_result
            or operator != "AND"
            or side != "right"
        ):
            return None

        # Only apply filters to the right side of AND operations
        logger.trace(f"Applying filter from previous result: {self.last_result}")
        return self.last_result

    def reset(self) -> None:
        self.scores = defaultdict(int)
        self.last_result = None
        self.current_side = None

    def updates_scores(self, doc_ids: Sequence[int], scores: Sequence[float]) -> None:
        logger.trace(f"Updating scores for {len(doc_ids)} documents")
        for doc_id, score in zip(doc_ids, scores, strict=True):
            self.scores[doc_id] += score

        for i, doc_id in enumerate(doc_ids):
            self.scores[doc_id] += scores[i]

    def percentileterm(self, items: list[Token]) -> set[int]:
        # TODO: Investigate length of items and annotations
        logger.trace(f"Evaluating percentile term: {items}")
        percentile = float(items[0].value)
        comparison = items[1].value
        reference = float(items[2].value)
        identifier = items[3].value if len(items) > 3 and isinstance(items[3], Token) else None
        operator = None
        side = None
        if len(items) >= 5:
            operator = items[-2]
            side = items[-1]

        result = self.fainder_index.search(
            percentile, comparison, reference, identifier, self._get_doc_filter(operator, side)
        )
        self.last_result = result
        return result

    def keywordterm(self, items: list[Token]) -> set[int]:
        # TODO: Investigate length of items and annotations
        logger.trace(f"Evaluating keyword term: {items}")
        keyword = items[0].value.strip()
        operator = items[-2] if len(items) > 2 else None
        side = items[-1] if len(items) > 2 else None
        filter_docs = self._get_doc_filter(operator, side)

        result, scores = self.lucene_connector.evaluate_query(keyword, filter_docs)
        self.updates_scores(result, scores)
        result_set = set(result)
        self.last_result = result_set
        return result_set

    def term(self, items: tuple[Token, set[int]]) -> set[int]:
        logger.trace(f"Evaluating term with items: {items}")
        return items[1]

    def not_expr(self, items: list[set[int]]) -> set[int]:
        logger.trace(f"Evaluating NOT expression with {len(items)} items")
        to_negate = items[0]
        return self.doc_ids - to_negate

    def expression(self, items: list[set[int]]) -> set[int]:
        logger.trace(f"Evaluating expression with {len(items[0])} items")
        return items[0]

    def query(self, items: list[set[int] | Token]) -> set[int]:
        logger.trace(f"Evaluating query with {len(items)} items")
        if len(items) == 1 and isinstance(items[0], set):
            return items[0]

        left: set[int] = items[0]  # type: ignore
        operator: str = items[1].value.strip()  # type: ignore
        right: set[int] = items[2]  # type: ignore

        match operator:
            case "AND":
                return left & right
            case "OR":
                return left | right
            case "XOR":
                return left ^ right
            case _:
                raise ValueError(f"Unknown operator: {operator}")

    def start(self, items: list[set[int]]) -> set[int]:
        logger.trace("Starting query evaluation")
        return items[0]
