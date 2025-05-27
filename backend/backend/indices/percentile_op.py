from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
from fainder.execution.new_runner import run_approx, run_exact
from fainder.utils import load_input
from loguru import logger

from backend.config import ColumnArray, FainderError, FainderMode

if TYPE_CHECKING:
    import numpy as np
    from fainder.typing import Histogram
    from fainder.typing import PercentileIndex as PctlIndex
    from fainder.typing import PercentileQuery as PctlQuery
    from numpy.typing import NDArray


class FainderIndex:
    def __init__(
        self,
        rebinning_path: Path | None,
        conversion_path: Path | None,
        histogram_path: Path | None,
    ) -> None:
        self.rebinning_index: tuple[list[PctlIndex], list[NDArray[np.float64]]] | None = (
            load_input(rebinning_path, "rebinning index") if rebinning_path else None
        )
        self.conversion_index: tuple[list[PctlIndex], list[NDArray[np.float64]]] | None = (
            load_input(conversion_path, "conversion index") if conversion_path else None
        )
        self.hists: list[tuple[np.uint32, Histogram]] | None = (
            load_input(histogram_path, "histograms") if histogram_path else None
        )

    def update(
        self,
        rebinning_path: Path | None,
        conversion_path: Path | None,
        histogram_path: Path | None,
    ) -> None:
        self.rebinning_index = (
            load_input(rebinning_path, "rebinning index") if rebinning_path else None
        )
        self.conversion_index = (
            load_input(conversion_path, "conversion index") if conversion_path else None
        )
        self.hists = load_input(histogram_path, "histograms") if histogram_path else None

    def search(
        self,
        percentile: float,
        comparison: str,
        reference: float,
        fainder_mode: FainderMode,
        hist_filter: ColumnArray | None = None,
    ) -> ColumnArray:
        # Data validation
        if not (0 < percentile <= 1) or comparison not in ["ge", "gt", "le", "lt"]:
            raise FainderError(
                f"Invalid percentile predicate: {percentile};{comparison};{reference}"
            )

        # Predicate evaluation
        result: ColumnArray
        query: PctlQuery = (percentile, comparison, reference)  # type: ignore
        match fainder_mode:
            case FainderMode.LOW_MEMORY:
                if self.rebinning_index is None:
                    raise FainderError("Rebinning index must be loaded for low_memory mode.")
                result, runtime = run_approx(
                    fainder_index=self.rebinning_index,
                    query=query,
                    index_mode="recall",
                    id_filter=hist_filter,
                )
            case FainderMode.FULL_PRECISION:
                if self.conversion_index is None:
                    raise FainderError("Conversion index must be loaded for full_precision mode.")
                result, runtime = run_approx(
                    fainder_index=self.conversion_index,
                    query=query,
                    index_mode="precision",
                    id_filter=hist_filter,
                )
            case FainderMode.FULL_RECALL:
                if self.conversion_index is None:
                    raise FainderError("Conversion index must be loaded for full_recall mode.")
                result, runtime = run_approx(
                    fainder_index=self.conversion_index,
                    query=query,
                    index_mode="recall",
                    id_filter=hist_filter,
                )
            case FainderMode.EXACT:
                if self.conversion_index is None or self.hists is None:
                    raise FainderError(
                        "Conversion index and histograms must be loaded for exact mode."
                    )

                result, runtime = run_exact(
                    fainder_index=self.conversion_index,
                    hists=self.hists,
                    query=query,
                    id_filter=hist_filter,
                )

        logger.info(
            "Query '{}' ({} mode) returned {} histograms in {} seconds. With filter size: {}",
            query,
            fainder_mode,
            len(result),
            f"{runtime:.2f}",
            hist_filter.size if hist_filter is not None else "no filter",
        )

        return result
