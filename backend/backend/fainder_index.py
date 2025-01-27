from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
from fainder.execution.runner import run
from fainder.utils import load_input
from loguru import logger

from backend.config import FainderError, FainderMode, Metadata

if TYPE_CHECKING:
    from fainder.typing import PercentileIndex as PctlIndex
    from fainder.typing import PercentileQuery as PctlQuery
    from numpy.typing import NDArray


class FainderIndex:
    def __init__(
        self, metadata: Metadata, rebinning_path: Path | None, conversion_path: Path | None
    ) -> None:
        self.doc_to_cols = metadata.doc_to_cols
        self.col_to_doc = metadata.col_to_doc
        self.col_to_hist = metadata.col_to_hist
        self.hist_to_col = metadata.hist_to_col

        self.rebinning_index: tuple[list[PctlIndex], list[NDArray[np.float64]]] | None = (
            (load_input(rebinning_path, "index")) if rebinning_path else None
        )
        self.conversion_index: tuple[list[PctlIndex], list[NDArray[np.float64]]] | None = (
            load_input(conversion_path, "index") if conversion_path else None
        )

    def update(
        self, metadata: Metadata, rebinning_path: Path | None, conversion_path: Path | None
    ) -> None:
        self.doc_to_cols = metadata.doc_to_cols
        self.col_to_doc = metadata.col_to_doc
        self.col_to_hist = metadata.col_to_hist
        self.hist_to_col = metadata.hist_to_col

        self.rebinning_index = load_input(rebinning_path, "index") if rebinning_path else None
        self.conversion_index = load_input(conversion_path, "index") if conversion_path else None

    def search(
        self,
        percentile: float,
        comparison: str,
        reference: float,
        fainder_mode: FainderMode,
        hist_filter: set[np.uint32] | None = None,
    ) -> set[np.uint32]:
        # Data validation
        if not (0 < percentile <= 1) or comparison not in ["ge", "gt", "le", "lt"]:
            raise FainderError(f"{percentile};{comparison};{reference}")

        filter_array = np.array(list(hist_filter), dtype=np.uint32) if hist_filter else None

        # Predicate evaluation
        query: PctlQuery = (percentile, comparison, reference)  # type: ignore
        match fainder_mode:
            case "low_memory":
                results, runtime = run(
                    self.rebinning_index,
                    queries=[query],
                    input_type="index",
                    index_mode="recall",
                    hist_filter=filter_array,
                )
            case "full_precision":
                results, runtime = run(
                    self.conversion_index,
                    queries=[query],
                    input_type="index",
                    index_mode="precision",
                    hist_filter=filter_array,
                )
            case "full_recall":
                results, runtime = run(
                    self.conversion_index,
                    queries=[query],
                    input_type="index",
                    index_mode="recall",
                    hist_filter=filter_array,
                )
            case "exact":
                raise NotImplementedError("Exact mode not implemented yet.")
            case _:
                raise FainderError(f"Invalid Fainder Mode: {fainder_mode}")

        result = results[0]
        logger.trace(f"Results: {result}")
        logger.info(f"Query '{query}' returned {len(result)} histograms in {runtime:.2f} seconds.")

        return result
