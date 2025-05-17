from numpy import uint32
from numpy.typing import NDArray
import numpy as np
from numba import jit

from backend.config import ColumnArray, DocumentArray

#@jit() # TODO: test performance
def doc_to_col_ids(doc_ids: DocumentArray, doc_to_cols: list[list[int]]) -> ColumnArray:
    # convert the document IDs to column IDs using a set comprehension
    result: list[uint32] = []
    for doc_id in doc_ids:
        result.extend(doc_to_cols[doc_id])

    cols_array = np.fromiter(result, dtype=uint32)
    return cols_array    

def col_to_doc_ids(col_ids: ColumnArray, col_to_doc: NDArray[uint32]) -> DocumentArray:
    # convert the column IDs to document IDs using numpy array operations
    result = np.unique(col_to_doc[col_ids])
    return result.astype(np.uint)


def col_to_hist_ids(col_ids: ColumnArray, cutoff_hists: int) -> ColumnArray:
    # filter out the columns that are under the cutoff using numpy array operations
    return col_ids[col_ids < cutoff_hists] 

