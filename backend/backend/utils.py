import json
import os
from typing import Any

import numpy as np
from fainder.typing import PercentileQuery
from loguru import logger

from backend.config import (
    DICT_FILE_ID_TO_HISTS,
    LIST_OF_DOCS,
    LIST_OF_HIST,
    METADATA,
    PATH_TO_METADATA,
)


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


def parse_precentile_query(query: str) -> PercentileQuery:
    split_query = query.split(";")
    assert len(split_query) == 3 or len(split_query) == 4

    percentile = float(split_query[0])
    assert 0 < percentile <= 1

    reference = float(split_query[2])

    assert split_query[1] in ["ge", "gt", "le", "lt"]
    comparison: Literal[le, lt, ge, gt] = split_query[1]  # type: ignore

    return percentile, comparison, reference


def get_metadata(doc_ids: list[int]) -> list[dict[str, Any]]:
    list_of_metadata_files = os.listdir(PATH_TO_METADATA)

    metadata = []

    for i in doc_ids:
        metadata_file = list_of_metadata_files[i]
        with open(os.path.join(PATH_TO_METADATA, metadata_file)) as f:
            metadata.append(json.load(f))

    return metadata


def combine_results(lucene_results: list[int], fainder_results: list[int]) -> list[int]:
    print(f"Lucene results: {lucene_results}")
    print(f"Fainder results: {fainder_results}")

    result = []

    for i in lucene_results:
        if i in fainder_results:
            result.append(i)

    return result
