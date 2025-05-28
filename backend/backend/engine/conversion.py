import numpy as np
from numpy.typing import NDArray

from backend.config import ColumnArray, DocumentArray


def doc_to_col_ids(doc_ids: DocumentArray, doc_to_cols: list[NDArray[np.uint32]]) -> ColumnArray:
    return np.fromiter(
        (col_id for doc_id in doc_ids for col_id in doc_to_cols[int(doc_id)]),
        dtype=np.uint32,
    )


def col_to_doc_ids(col_ids: ColumnArray, col_to_doc: NDArray[np.uint32]) -> DocumentArray:
    return np.unique(col_to_doc[col_ids])


def col_to_hist_ids(col_ids: ColumnArray, cutoff_hists: int) -> ColumnArray:
    # filter out the columns that are under the cutoff using numpy array operations
    return col_ids[col_ids < cutoff_hists]
