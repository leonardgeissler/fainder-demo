import numpy as np
from loguru import logger

from backend.config import DICT_FILE_ID_TO_HISTS, LIST_OF_DOCS, LIST_OF_HIST, METADATA


def get_hists_for_doc_ids(doc_ids: list[int]) -> set[np.uint32]:
    hists_int = []

    for i in doc_ids:
        try:
            hists_int.extend(DICT_FILE_ID_TO_HISTS[str(i)])
        except KeyError:
            logger.error(f"Could not find histogram for doc id: {i}")

    return {np.uint32(i) for i in hists_int}


def number_of_matching_histograms_to_doc_number(matching_histograms: set[np.uint32]) -> list[int]:
    """
    This function will take a set of histogram ids and return a list of document ids.
    """

    doc_ids = set()
    for i in matching_histograms:
        split = LIST_OF_HIST[i].split("&")
        doc_id = split[2]
        doc_ids.add(LIST_OF_DOCS.index(doc_id))
    return list(doc_ids)


def get_histogram_ids_from_identifer(identifer: str) -> set[np.uint32]:
    """
    This function will take a column name and return a set of histogram ids.
    """
    if not isinstance(METADATA, dict) or "column_names" not in METADATA:
        logger.error("Metadata is not loaded")
        return set()
    column_names: dict[str, list[str]] = METADATA["column_names"]
    histogram_ids: set[np.uint32] = set()
    try:
        histogram_strings = column_names[identifer]
    except KeyError:
        return histogram_ids
    for hist_str in histogram_strings:
        histogram_ids.add(np.uint32(LIST_OF_HIST.index(hist_str)))
    return histogram_ids
