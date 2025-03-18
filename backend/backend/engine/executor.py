from os import read, write
from pydoc import doc
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Sequence
from functools import reduce
from operator import and_, or_
from typing import Any, TypeGuard, TypeVar
from unittest import result

from lark import ParseTree, Token, Transformer
from loguru import logger
from numpy import uint32
from numpy.typing import NDArray

from backend.config import (
    ColumnHighlights,
    DocumentHighlights,
    ExecutorType,
    FainderMode,
    Highlights,
    Metadata,
)
from backend.indices import FainderIndex, HnswIndex, TantivyIndex

T = TypeVar("T", tuple[set[int], Highlights], set[uint32])


class BaseExecutor(Transformer[Token, tuple[set[int], Highlights]], ABC):
    """Base abstract class for query executors that defines the common interface."""

    scores: dict[int, float]

    @abstractmethod
    def reset(
        self,
        fainder_mode: FainderMode,
        enable_highlighting: bool = False,
    ) -> None:
        """Reset the executor's state."""

    @abstractmethod
    def __init__(
        self,
        tantivy_index: TantivyIndex,
        fainder_index: FainderIndex,
        hnsw_index: HnswIndex,
        metadata: Metadata,
        fainder_mode: FainderMode = FainderMode.LOW_MEMORY,
        enable_highlighting: bool = False,
    ) -> None:
        pass


class SimpleExecutor(BaseExecutor):
    """This transformer evaluates a parse tree bottom-up and computes the query result."""

    fainder_mode: FainderMode
    scores: dict[int, float]

    def __init__(
        self,
        tantivy_index: TantivyIndex,
        fainder_index: FainderIndex,
        hnsw_index: HnswIndex,
        metadata: Metadata,
        fainder_mode: FainderMode = FainderMode.LOW_MEMORY,
        enable_highlighting: bool = False,
    ) -> None:
        self.tantivy_index = tantivy_index
        self.fainder_index = fainder_index
        self.hnsw_index = hnsw_index
        self.metadata = metadata

        self.reset(fainder_mode, enable_highlighting)

    def reset(
        self,
        fainder_mode: FainderMode,
        enable_highlighting: bool = False,
    ) -> None:
        self.scores = defaultdict(float)
        self.fainder_mode = fainder_mode
        self.enable_highlighting = enable_highlighting

    def updates_scores(self, doc_ids: Sequence[int], scores: Sequence[float]) -> None:
        logger.trace(f"Updating scores for {len(doc_ids)} documents")

        for doc_id, score in zip(doc_ids, scores, strict=True):
            self.scores[doc_id] += score

        for i, doc_id in enumerate(doc_ids):
            self.scores[doc_id] += scores[i]

    ### Operator implementations ###

    def keyword_op(self, items: list[Token]) -> tuple[set[int], Highlights]:
        logger.trace(f"Evaluating keyword term: {items}")

        result_docs, scores, highlights = self.tantivy_index.search(
            items[0], self.enable_highlighting
        )
        self.updates_scores(result_docs, scores)

        return set(result_docs), (highlights, set())  # Return empty set for column highlights

    def col_op(self, items: list[set[uint32]]) -> tuple[set[int], Highlights]:
        logger.trace(f"Evaluating column term: {items}")

        if len(items) != 1:
            raise ValueError("Column term must have exactly one item")
        col_ids = items[0]
        doc_ids = col_to_doc_ids(col_ids, self.metadata.col_to_doc)
        if self.enable_highlighting:
            return doc_ids, ({}, col_ids)

        return doc_ids, ({}, set())

    def name_op(self, items: list[Token]) -> set[uint32]:
        logger.trace(f"Evaluating column term: {items}")

        column = items[0]
        k = int(items[1])

        result = self.hnsw_index.search(column, k, None)

        return result  # noqa: RET504

    def percentile_op(self, items: list[Token]) -> set[uint32]:
        logger.trace(f"Evaluating percentile term: {items}")

        percentile = float(items[0])
        comparison: str = items[1]
        reference = float(items[2])

        result_hists = self.fainder_index.search(
            percentile, comparison, reference, self.fainder_mode
        )
        return hist_to_col_ids(result_hists, self.metadata.hist_to_col)

    def conjunction(
        self, items: list[tuple[set[int], Highlights]] | list[set[uint32]]
    ) -> tuple[set[int], Highlights] | set[uint32]:
        logger.trace(f"Evaluating conjunction with items: {items}")

        return junction(items, and_, self.enable_highlighting, self.metadata.doc_to_cols)

    def disjunction(
        self, items: list[tuple[set[int], Highlights]] | list[set[uint32]]
    ) -> tuple[set[int], Highlights] | set[uint32]:
        logger.trace(f"Evaluating disjunction with items: {items}")

        return junction(items, or_, self.enable_highlighting, self.metadata.doc_to_cols)

    def negation(self, items: list[T]) -> T:
        logger.trace(f"Evaluating negation with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Negation term must have exactly one item")
        if isinstance(items[0], tuple):
            to_negate, _ = items[0]
            all_docs = set(self.metadata.doc_to_cols.keys())
            # Result highlights are reset for negated results
            doc_highlights: DocumentHighlights = {}
            col_highlights: ColumnHighlights = set()
            return all_docs - to_negate, (doc_highlights, col_highlights)

        to_negate_cols = items[0]
        # For column expressions, we negate using the set of all column IDs
        all_columns = {uint32(col_id) for col_id in range(len(self.metadata.col_to_doc))}
        return all_columns - to_negate_cols

    def query(self, items: list[tuple[set[int], Highlights]]) -> tuple[set[int], Highlights]:
        logger.trace(f"Evaluating query with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Query must have exactly one item")
        return items[0]

class IntermediateResults:
    """Intermediate results for prefiltering. Only one of doc_ids, col_ids, or hist_ids should be set. 
    If multiple are set, this should result in an error.
    """
    doc_ids: set[int] | None = None
    col_ids: set[uint32] | None = None
    hist_ids: set[uint32] | None = None

def _get_write_group(tree: ParseTree | Token, optimizer_rules: list[IntermediaryResultGroups]) -> int:
    """Get the write group for a tree or token using the IntermediaryResultGroups rule."""
    for rule in optimizer_rules:
        if isinstance(rule, IntermediaryResultGroups):
            return rule.get_write_group(tree)
    raise ValueError("No IntermediaryResultGroups rule found in optimizer_rules")

def _get_read_groups(tree: ParseTree | Token, optimizer_rules: list[IntermediaryResultGroups]) -> list[int]:
    """Get the read groups for a tree or token using the IntermediaryResultGroups rule."""
    for rule in optimizer_rules:
        if isinstance(rule, IntermediaryResultGroups):
            return rule.get_read_groups(tree)
    raise ValueError("No IntermediaryResultGroups rule found in optimizer_rules")

class PrefilteringExecutor(BaseExecutor):
    """Uses prefiltering to reduce the number of documents before executing the query."""
    fainder_mode: FainderMode
    scores: dict[int, float]
    intermediate_results: list[IntermediateResults]

    def __init__(
        self,
        tantivy_index: TantivyIndex,
        fainder_index: FainderIndex,
        hnsw_index: HnswIndex,
        metadata: Metadata,
        fainder_mode: FainderMode = FainderMode.LOW_MEMORY,
        enable_highlighting: bool = False,
        optimizer_rules: list[IntermediaryResultGroups] = None,
    ) -> None:
        self.tantivy_index = tantivy_index
        self.fainder_index = fainder_index
        self.hnsw_index = hnsw_index
        self.metadata = metadata
        self.optimizer_rules = optimizer_rules or []

        self.reset(fainder_mode, enable_highlighting)

    def reset(
        self,
        fainder_mode: FainderMode,
        enable_highlighting: bool = False,
    ) -> None:
        self.scores = defaultdict(float)
        self.fainder_mode = fainder_mode
        self.enable_highlighting = enable_highlighting

    def updates_scores(self, doc_ids: Sequence[int], scores: Sequence[float]) -> None:
        logger.trace(f"Updating scores for {len(doc_ids)} documents")

        for doc_id, score in zip(doc_ids, scores, strict=True):
            self.scores[doc_id] += score

        for i, doc_id in enumerate(doc_ids):
            self.scores[doc_id] += scores[i]

    def _update_intermediate_results(self, result: IntermediateResults, write_group: int) -> None:
        if write_group >= len(self.intermediate_results):
            self.intermediate_results.extend([IntermediateResults() for _ in range(write_group + 1)])

        if result.doc_ids is not None:
            self.intermediate_results[write_group].doc_ids = None
            self.intermediate_results[write_group].hist_ids = None
            self.intermediate_results[write_group].doc_ids = result.doc_ids
        if result.col_ids is not None:
            self.intermediate_results[write_group].doc_ids = None
            self.intermediate_results[write_group].hist_ids = None
            self.intermediate_results[write_group].col_ids = result.col_ids
        if result.hist_ids is not None:
            self.intermediate_results[write_group].doc_ids = None
            self.intermediate_results[write_group].col_ids = None
            self.intermediate_results[write_group].hist_ids = result.hist_ids

    def _get_hist_ids_from_read_groups(self, read_groups: list[int]) -> set[uint32]:
        hist_ids: set[uint32] = set()
        if len(read_groups) == 0:
            return hist_ids
        for read_group in read_groups:
            if self.intermediate_results[read_group].hist_ids is not None:
                hist_ids |= self.intermediate_results[read_group].hist_ids
            if self.intermediate_results[read_group].col_ids is not None:
                hist_ids |= col_to_hist_ids(self.intermediate_results[read_group].col_ids, self.metadata.col_to_hist)
            if self.intermediate_results[read_group].doc_ids is not None:
                hist_ids |= col_to_hist_ids(doc_to_col_ids(self.intermediate_results[read_group].doc_ids, self.metadata.doc_to_cols), self.metadata
                .col_to_hist)
        return hist_ids

    ### Operator implementations ###

    def keyword_op(self, items: list[Token]) -> tuple[set[int], Highlights, int]:
        logger.trace(f"Evaluating keyword term: {items}")

        result_docs, scores, highlights = self.tantivy_index.search(
            items[0], self.enable_highlighting
        )
        self.updates_scores(result_docs, scores)

        result = IntermediateResults()
        result.doc_ids = set(result_docs)
        write_group = _get_write_group(items[0], self.optimizer_rules)
        self._update_intermediate_results(result, write_group)

        return set(result_docs), (highlights, set()), write_group  # Return empty set for column highlights

    def col_op(self, items: list[tuple[set[uint32] , int]]) -> tuple[set[int], Highlights, int]:
        logger.trace(f"Evaluating column term: {items}")

        if len(items) != 1:
            raise ValueError("Column term must have exactly one item")
        col_ids = items[0][0]
        write_group = items[0][1]
        doc_ids = col_to_doc_ids(col_ids, self.metadata.col_to_doc)
        if self.enable_highlighting:
            return doc_ids, ({}, col_ids), write_group

        return doc_ids, ({}, set()), write_group

    def name_op(self, items: list[Token]) -> tuple[set[uint32], int]:
        logger.trace(f"Evaluating column term: {items}")

        column = items[0]
        k = int(items[1])

        result = self.hnsw_index.search(column, k, None)
        intermediate = IntermediateResults()
        intermediate.col_ids = result
        write_group = _get_write_group(items[0], self.optimizer_rules)
        self._update_intermediate_results(intermediate, write_group)
        return result, write_group  # noqa: RET504

    def percentile_op(self, items: list[Token]) -> tuple[set[uint32], int]:
        logger.trace(f"Evaluating percentile term: {items}")

        percentile = float(items[0])
        comparison: str = items[1]
        reference = float(items[2])
        hist_filter = self._get_hist_ids_from_read_groups(_get_read_groups(items[0], self.optimizer_rules))
        write_group = _get_write_group(items[0], self.optimizer_rules)
        if len(hist_filter) == 0:
            return set(), write_group
        result_hists = self.fainder_index.search(
            percentile, comparison, reference, self.fainder_mode, hist_filter
        )
        result = hist_to_col_ids(result_hists, self.metadata.hist_to_col) 
        intermediate = IntermediateResults()
        intermediate.hist_ids = result_hists
        self._update_intermediate_results(intermediate, write_group)
        return result, write_group

    def conjunction(
        self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]
    ) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating conjunction with items: {items}")

        clean_items: list[tuple[set[int], Highlights]] | list[set[uint32]] = []
        for item in items:
            if len(item) == 3:
                clean_items.append((item[0], item[1]))
            else:
                clean_items.append((item[0]))

        result = junction(clean_items, and_, self.enable_highlighting, self.metadata.doc_to_cols)
        intermediate = IntermediateResults()
        if isinstance(result, tuple):
            intermediate.doc_ids = result[0]
        else:
            intermediate.col_ids = result
        if len(items[0]) == 3:
            write_group = items[0][2]
        else:
            write_group = items[0][1]
        self._update_intermediate_results(intermediate, write_group)
        if isinstance(result, tuple):
            return result[0], result[1], write_group
        return result, write_group
    

    def disjunction(
        self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]
    ) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating disjunction with items: {items}")

        clean_items: list[tuple[set[int], Highlights]] | list[set[uint32]] = []
        for item in items:
            if len(item) == 3:
                clean_items.append((item[0], item[1]))
            else:
                clean_items.append((item[0]))

        result = junction(clean_items, or_, self.enable_highlighting, self.metadata.doc_to_cols)
        intermediate = IntermediateResults()
        if isinstance(result, tuple):
            intermediate.doc_ids = result[0]
        else:
            intermediate.col_ids = result
        if len(items[0]) == 3:
            write_group = items[0][2]
        else:
            write_group = items[0][1]
        write_group = write_group - 1 # Very important!
        self._update_intermediate_results(intermediate, write_group)
        if isinstance(result, tuple):
            return result[0], result[1], write_group
        return result, write_group
    
    def negation(self, items: list[tuple[set[int], Highlights, int]] | list[tuple[set[uint32], int]]) -> tuple[set[int], Highlights, int] | tuple[set[uint32], int]:
        logger.trace(f"Evaluating negation with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Negation term must have exactly one item")
        if isinstance(items[0], tuple):
            if len(items[0]) == 3:
                to_negate, _, write_group = items[0]
                write_group = write_group - 1 # Very important!
                all_docs = set(self.metadata.doc_to_cols.keys())
                # Result highlights are reset for negated results
                doc_highlights: DocumentHighlights = {}
                col_highlights: ColumnHighlights = set()
                result = (all_docs - to_negate, (doc_highlights, col_highlights), write_group)
                intermediate = IntermediateResults()
                intermediate.doc_ids = result[0]
                self._update_intermediate_results(intermediate, write_group)
                return result
            elif len(items[0]) == 2:
                to_negate_cols: set[uint32] = items[0][0]
                write_group_cols: int = items[0][1]
                # For column expressions, we negate using the set of all column IDs
                all_columns = {uint32(col_id) for col_id in range(len(self.metadata.col_to_doc))}
                result = all_columns - to_negate_cols
                intermediate = IntermediateResults()
                intermediate.col_ids = result
                self._update_intermediate_results(intermediate, write_group_cols)
                return result, write_group_cols
            
        raise ValueError("Negation term must have exactly one item")

    def query(self, items: list[tuple[set[int], Highlights, int]]) -> tuple[set[int], Highlights]:
        logger.trace(f"Evaluating query with {len(items)} items")

        if len(items) != 1:
            raise ValueError("Query must have exactly one item")
        return items[0][0], items[0][1]

def create_executor(
    executor_type: ExecutorType,
    tantivy_index: TantivyIndex,
    fainder_index: FainderIndex,
    hnsw_index: HnswIndex,
    metadata: Metadata,
    fainder_mode: FainderMode = FainderMode.LOW_MEMORY,
    enable_highlighting: bool = False,
) -> BaseExecutor:
    """Factory function to create the appropriate executor based on the executor type."""
    if executor_type == ExecutorType.SIMPLE:
        return SimpleExecutor(
            tantivy_index=tantivy_index,
            fainder_index=fainder_index,
            hnsw_index=hnsw_index,
            metadata=metadata,
            fainder_mode=fainder_mode,
            enable_highlighting=enable_highlighting,
        )
    if executor_type == ExecutorType.PREFILTERING:
        return PrefilteringExecutor(
            tantivy_index=tantivy_index,
            fainder_index=fainder_index,
            hnsw_index=hnsw_index,
            metadata=metadata,
            fainder_mode=fainder_mode,
            enable_highlighting=enable_highlighting,
        )
    if executor_type == ExecutorType.PARALLEL:
        # TODO: Implement ParallelExecutor
        raise NotImplementedError("ParallelExecutor not implemented yet")
    raise ValueError(f"Unknown executor type: {executor_type}")


def is_table_result(val: list[Any]) -> TypeGuard[list[tuple[set[int], Highlights]]]:
    """Check if a list contains table results (document IDs and highlights)."""
    return all(isinstance(item, tuple) for item in val)


def merge_highlights(
    left: Highlights, right: Highlights, doc_ids: set[int], doc_to_cols: dict[int, set[int]]
) -> Highlights:
    """Merge highlights for documents that are in the result set."""
    pattern = r"<mark>(.*?)</mark>"
    regex = re.compile(pattern, re.DOTALL)

    # Merge document highlights
    doc_highlights: DocumentHighlights = {}
    left_doc_highlights = left[0]
    right_doc_highlights = right[0]
    for doc_id in doc_ids:
        left_highlights = left_doc_highlights.get(doc_id, {})
        right_highlights = right_doc_highlights.get(doc_id, {})

        # Only process if either side has highlights
        if left_highlights or right_highlights:
            merged_highlights = {}

            # Process each field that appears in either highlight set
            all_keys = set(left_highlights.keys()) | set(right_highlights.keys())
            for key in all_keys:
                left_text = left_highlights.get(key, "")
                right_text = right_highlights.get(key, "")

                # If either text is empty, use the non-empty one
                if not left_text:
                    merged_highlights[key] = right_text
                    continue
                if not right_text:
                    continue

                # Both texts have content, merge their marks
                # Extract all marked words from right text
                right_marks = set(regex.findall(right_text))

                # Add marks from right text to left text
                for word in right_marks:
                    if f"<mark>{word}</mark>" not in left_text:
                        # Word isn't marked yet
                        left_text = left_text.replace(word, f"<mark>{word}</mark>")

                merged_highlights[key] = left_text

            doc_highlights[doc_id] = merged_highlights

    # Merge column highlights
    col_highlights = left[1] | right[1]
    col_highlights &= doc_to_col_ids(doc_ids, doc_to_cols)

    return doc_highlights, col_highlights


def junction(
    items: list[tuple[set[int], Highlights]] | list[set[uint32]],
    operator: Callable[[Any, Any], Any],
    enable_highlighting: bool = False,
    doc_to_cols: dict[int, set[int]] | None = None,
) -> tuple[set[int], Highlights] | set[uint32]:
    """Combine query results using a junction operator (AND/OR)."""
    if len(items) < 2:
        raise ValueError("Junction must have at least two items")

    # Items contains table results (i.e., tuple[set[int], Highlights])
    if is_table_result(items):
        if enable_highlighting and doc_to_cols is not None:
            # Initialize result with first item
            doc_ids: set[int] = items[0][0]
            highlights: Highlights = items[0][1]

            # Merge all other items
            for item in items[1:]:
                doc_ids = operator(doc_ids, item[0])
                highlights = merge_highlights(highlights, item[1], doc_ids, doc_to_cols)

            return doc_ids, highlights

        return reduce(operator, [item[0] for item in items]), ({}, set())

    # Items contains column results (i.e., set[uint32])
    return reduce(operator, items)  # type: ignore


def doc_to_col_ids(doc_ids: set[int], doc_to_cols: dict[int, set[int]]) -> set[uint32]:
    return {
        uint32(col_id)
        for doc_id in doc_ids
        if doc_id in doc_to_cols
        for col_id in doc_to_cols[doc_id]
    }


def col_to_doc_ids(col_ids: set[uint32], col_to_doc: NDArray[uint32]) -> set[int]:
    return {int(col_to_doc[col_id]) for col_id in col_ids}


def col_to_hist_ids(col_ids: set[uint32], col_to_hist: dict[int, int]) -> set[uint32]:
    return {uint32(col_to_hist[int(col_id)]) for col_id in col_ids if int(col_id) in col_to_hist}


def hist_to_col_ids(hist_ids: set[uint32], hist_to_col: NDArray[uint32]) -> set[uint32]:
    return {hist_to_col[hist_id] for hist_id in hist_ids}
