from collections import defaultdict
from operator import and_, or_

from lark import ParseTree, Token, Transformer
from loguru import logger
from numpy import uint32

from backend.config import (
    ColumnHighlights,
    DocumentHighlights,
    FainderMode,
    Highlights,
    Metadata,
)
from backend.engine.conversion import (
    col_to_doc_ids,
    col_to_hist_ids,
    doc_to_col_ids,
    hist_to_col_ids,
)
from backend.indices import FainderIndex, HnswIndex, TantivyIndex

from .common import ResultGroupAnnotator, exceeds_filtering_limit, junction
from .executor import Executor


class IntermediateResult:
    """Intermediate results for prefiltering.
    Only one of doc_ids or col_ids should be set.
    If multiple are set, this should result in an error.
    """

    def __init__(
        self, doc_ids: set[int] | None = None, col_ids: set[uint32] | None = None
    ) -> None:
        if doc_ids is None and col_ids is None:
            raise ValueError("doc_ids and col_ids cannot both be None")
        if doc_ids is not None and col_ids is not None:
            raise ValueError("doc_ids and col_ids cannot both be set")

        self._col_ids: set[uint32] | None = col_ids
        self._doc_ids: set[int] | None = doc_ids

    def add_col_ids(self, col_ids: set[uint32]) -> None:
        self._col_ids = col_ids
        self._doc_ids = None

    def add_doc_ids(self, doc_ids: set[int]) -> None:
        self._doc_ids = doc_ids
        self._col_ids = None

    def build_hist_filter(
        self, metadata: Metadata, fainder_mode: FainderMode
    ) -> set[uint32] | None:
        """Build a histogram filter from the intermediate results."""
        if self._col_ids is not None:
            if exceeds_filtering_limit(self._col_ids, "num_col_ids", fainder_mode):
                return None
            return col_to_hist_ids(self._col_ids, metadata.col_to_hist)
        if self._doc_ids is not None:
            if exceeds_filtering_limit(self._doc_ids, "num_doc_ids", fainder_mode):
                return None
            col_ids = doc_to_col_ids(self._doc_ids, metadata.doc_to_cols)
            return col_to_hist_ids(col_ids, metadata.col_to_hist)
        return None

    def __str__(self) -> str:
        """String representation of the intermediate result."""
        return f"IntermediateResult(\n\tdoc_ids={self._doc_ids},\n\tcol_ids={self._col_ids}\n)"


class IntermediateResultStore:
    """Store intermediate results for prefiltering per group."""

    def __init__(self) -> None:
        self.results: dict[int, IntermediateResult] = {}

    def add_col_id_results(self, write_group: int, col_ids: set[uint32]) -> None:
        logger.trace(f"Adding column IDs to write group {write_group}: {col_ids}")
        if write_group in self.results:
            self.results[write_group].add_col_ids(col_ids=col_ids)
        else:
            self.results[write_group] = IntermediateResult(col_ids=col_ids)

    def add_doc_id_results(self, write_group: int, doc_ids: set[int]) -> None:
        logger.trace(f"Adding document IDs to write group {write_group}: {doc_ids}")
        if write_group in self.results:
            self.results[write_group].add_doc_ids(doc_ids=doc_ids)
        else:
            self.results[write_group] = IntermediateResult(doc_ids=doc_ids)

    def build_hist_filter(
        self, read_groups: list[int], metadata: Metadata, fainder_mode: FainderMode
    ) -> set[uint32] | None:
        """Build a histogram filter from the intermediate results."""
        hist_filter: set[uint32] | None = None
        if len(read_groups) == 0:
            raise ValueError("Cannot build a hist filter without read groups")

        for read_group in read_groups:
            if read_group not in self.results:
                # This means this group does not have an intermediate result yet this happens alot
                continue

            logger.trace(
                f"Processing read group {read_group} with results {self.results[read_group]}"
            )
            intermediate = self.results[read_group].build_hist_filter(metadata, fainder_mode)
            logger.trace(f"intermediate {intermediate}")

            if intermediate is None:
                return None
            if len(intermediate) == 0:
                return set()

            if hist_filter is None:
                hist_filter = intermediate
            else:
                hist_filter &= intermediate

        logger.trace(f"Hist filter: {hist_filter}")
        return hist_filter


class PrefilteringExecutor(Transformer[Token, tuple[set[int], Highlights]], Executor):
    """Uses prefiltering to reduce the number of documents before executing the query."""

    def __init__(
        self,
        tantivy_index: TantivyIndex,
        fainder_index: FainderIndex,
        hnsw_index: HnswIndex,
        metadata: Metadata,
        fainder_mode: FainderMode = FainderMode.LOW_MEMORY,
        enable_highlighting: bool = False,
        min_usability_score: float = 0.0,
        rank_by_usability: bool = True,
    ) -> None:
        self.tantivy_index = tantivy_index
        self.fainder_index = fainder_index
        self.hnsw_index = hnsw_index
        self.metadata = metadata
        self.min_usability_score = min_usability_score
        self.rank_by_usability = rank_by_usability

        self.reset(fainder_mode, enable_highlighting)

    def reset(
        self,
        fainder_mode: FainderMode,
        enable_highlighting: bool = False,
    ) -> None:
        logger.trace("Resetting executor")
        self.scores: dict[int, float] = defaultdict(float)
        self.fainder_mode = fainder_mode
        self.enable_highlighting = enable_highlighting
        self.intermediate_results = IntermediateResultStore()
        self.write_groups: dict[int, int] = {}
        self.read_groups: dict[int, list[int]] = {}
        self.parent_write_group: dict[int, int] = {}

    def _get_write_group(self, node: ParseTree | Token) -> int:
        """Get the write group for a node."""
        node_id = id(node)
        if node_id in self.write_groups:
            return self.write_groups[node_id]
        logger.warning(f"Node {node} does not have a write group with id {node_id}")
        logger.warning(f"Write groups: {self.write_groups}")
        raise ValueError("Node does not have a write group")

    def _get_read_groups(self, node: ParseTree | Token) -> list[int]:
        """Get the read groups for a node."""
        node_id = id(node)
        if node_id in self.read_groups:
            return self.read_groups[node_id]
        logger.warning(f"Node {node} does not have read groups")
        logger.warning(f"Read groups: {self.read_groups}")
        raise ValueError("Node does not have read groups")

    def _get_parent_write_group(self, write_group: int) -> int:
        """Get the parent write group for a write group."""
        if write_group in self.parent_write_group:
            return self.parent_write_group[write_group]
        logger.warning(f"Write group {write_group} does not have a parent write group")
        logger.warning(f"Parent write groups: {self.parent_write_group}")
        raise ValueError("Write group does not have a parent write group")

    def execute(self, tree: ParseTree) -> tuple[set[int], Highlights]:
        """Start processing the parse tree."""
        self.write_groups = {}
        self.read_groups = {}
        logger.trace(tree.pretty())
        groups = ResultGroupAnnotator()
        groups.apply(tree)
        self.write_groups = groups.write_groups
        self.read_groups = groups.read_groups
        self.parent_write_group = groups.parent_write_group
        logger.trace(f"Write groups: {self.write_groups}")
        logger.trace(f"Read groups: {self.read_groups}")
        logger.trace(f"Parent write groups: {self.parent_write_group}")
        return self.transform(tree)

    ### Operator implementations ###

    def keyword_op(self, items: list[Token]) -> tuple[set[int], Highlights, int]:
        logger.trace(f"Evaluating keyword term: {items}")

        result_docs, scores, highlights = self.tantivy_index.search(
            items[0], self.enable_highlighting, self.min_usability_score, self.rank_by_usability
        )
        self.updates_scores(result_docs, scores)

        write_group = self._get_write_group(items[0])
        self.intermediate_results.add_doc_id_results(write_group, set(result_docs))

        parent_write_group = self._get_parent_write_group(write_group)

        return set(result_docs), (highlights, set()), parent_write_group

    def col_op(self, items: list[tuple[set[uint32], int]]) -> tuple[set[int], Highlights, int]:
        logger.trace(f"Evaluating column term: {items}")

        if len(items) != 1:
            raise ValueError("Column term must have exactly one item")
        col_ids = items[0][0]
        write_group = items[0][1]
        doc_ids = col_to_doc_ids(col_ids, self.metadata.col_to_doc)
        self.intermediate_results.add_doc_id_results(write_group, doc_ids)
        parent_write_group = self._get_parent_write_group(write_group)
        if self.enable_highlighting:
            return doc_ids, ({}, col_ids), parent_write_group

        return doc_ids, ({}, set()), parent_write_group

    def name_op(self, items: list[Token]) -> tuple[set[uint32], int]:
        logger.trace(f"Evaluating column term: {items}")

        column = items[0]
        k = int(items[1])

        result = self.hnsw_index.search(column, k, None)

        write_group = self._get_write_group(items[0])
        self.intermediate_results.add_col_id_results(write_group, result)
        parent_write_group = self._get_parent_write_group(write_group)
        return result, parent_write_group

    def percentile_op(self, items: list[Token]) -> tuple[set[uint32], int]:
        logger.trace(f"Evaluating percentile term: {items}")

        percentile = float(items[0])
        comparison: str = items[1]
        reference = float(items[2])
        hist_filter = self.intermediate_results.build_hist_filter(
            self._get_read_groups(items[0]), self.metadata, self.fainder_mode
        )
        logger.trace(f"Hist filter: {hist_filter}")
        write_group = self._get_write_group(items[0])
        if hist_filter is not None and len(hist_filter) == 0:
            return set(), write_group
        result_hists = self.fainder_index.search(
            percentile, comparison, reference, self.fainder_mode, hist_filter
        )
        result = hist_to_col_ids(result_hists, self.metadata.hist_to_col)
        self.intermediate_results.add_col_id_results(write_group, result)
        parent_write_group = self._get_parent_write_group(write_group)
        return result, parent_write_group

    def conjunction(
        self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]
    ) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating conjunction with items: {items}")

        clean_items: list[tuple[set[int], Highlights]] | list[set[uint32]] = []
        for item in items:
            if len(item) == 3:
                clean_items.append((item[0], item[1]))  # type: ignore
            else:
                clean_items.append(item[0])  # type: ignore

        result = junction(clean_items, and_, self.enable_highlighting, self.metadata.doc_to_cols)
        write_group = items[0][2] if len(items[0]) == 3 else items[0][1]
        if isinstance(result, tuple):
            self.intermediate_results.add_doc_id_results(write_group, result[0])
        else:
            self.intermediate_results.add_col_id_results(write_group, result)

        parent_write_group = self._get_parent_write_group(write_group)
        if isinstance(result, tuple):
            return result[0], result[1], parent_write_group
        return result, parent_write_group

    def disjunction(
        self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]
    ) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating disjunction with items: {items}")

        clean_items: list[tuple[set[int], Highlights]] | list[set[uint32]] = []
        for item in items:
            if len(item) == 3:
                clean_items.append((item[0], item[1]))  # type: ignore
            else:
                clean_items.append(item[0])  # type: ignore

        result = junction(clean_items, or_, self.enable_highlighting, self.metadata.doc_to_cols)
        write_group = items[0][2] if len(items[0]) == 3 else items[0][1]

        if isinstance(result, tuple):
            self.intermediate_results.add_doc_id_results(write_group, result[0])
        else:
            self.intermediate_results.add_col_id_results(write_group, result)

        parent_write_group = self._get_parent_write_group(write_group)
        if isinstance(result, tuple):
            return result[0], result[1], parent_write_group
        return result, parent_write_group

    def negation(
        self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]
    ) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating negation with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Negation term must have exactly one item")
        if len(items[0]) == 3:
            to_negate, _, write_group = items[0]
            all_docs = set(self.metadata.doc_to_cols.keys())
            # Result highlights are reset for negated results
            doc_highlights: DocumentHighlights = {}
            col_highlights: ColumnHighlights = set()
            parent_write_group = self._get_parent_write_group(write_group)
            result = (all_docs - to_negate, (doc_highlights, col_highlights), parent_write_group)
            self.intermediate_results.add_doc_id_results(write_group, result[0])
            return result
        if len(items[0]) == 2:
            to_negate_cols: set[uint32] = items[0][0]
            write_group_cols: int = items[0][1]
            # For column expressions, we negate using the set of all column IDs
            all_columns = {uint32(col_id) for col_id in range(len(self.metadata.col_to_doc))}
            result_col = all_columns - to_negate_cols
            self.intermediate_results.add_col_id_results(write_group_cols, result_col)
            parent_write_group_cols = self._get_parent_write_group(write_group_cols)
            return result_col, parent_write_group_cols

        raise ValueError("Negation term must have exactly one item")

    def query(self, items: list[tuple[set[int], Highlights, int]]) -> tuple[set[int], Highlights]:
        logger.trace(f"Evaluating query with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Query must have exactly one item")
        return items[0][0], items[0][1]
