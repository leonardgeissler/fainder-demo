import json
import os
from typing import Any, Literal

from fainder.execution.runner import run
from fainder.typing import PercentileQuery
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from fainder_demo.config import INDEX, LIST_OF_DOCS, PATH_TO_METADATA
from fainder_demo.percentile_grammar import (
    call_lucene_server,
    evaluate_query,
    number_of_matching_histograms_to_doc_number,
)
from fainder_demo.utils import get_hists_for_doc_ids

app = FastAPI()
origins = [
    "http://localhost:3000",  # Frontend development server
    "http://127.0.0.1:3000",  # Alternate localhost
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def parse_precentile_query(query: str) -> PercentileQuery:
    split_query = query.split(";")
    assert len(split_query) == 3 or len(split_query) == 4

    percentile = float(split_query[0])
    assert 0 < percentile <= 1

    reference = float(split_query[2])

    assert split_query[1] in ["ge", "gt", "le", "lt"]
    comparison: Literal["le", "lt", "ge", "gt"] = split_query[1]  # type: ignore

    return percentile, comparison, reference


@app.post("/query")
def query(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    print(f"Query: {percentile}")
    query = parse_precentile_query(percentile)

    # numbers of matching histograms
    result = run(INDEX, [query], "index")

    matching_histograms = result[0]

    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])

    bit_array = [1 if i in matching_docs else 0 for i in range(len(LIST_OF_DOCS))]

    return {"query": percentile, "bit_array": bit_array}


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


@app.post("/search")
def search(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords)  # document ids

    if len(results_keywords) == 0:
        return {"query": percentile, "keywords": keywords, "results": []}

    filter_hist = get_hists_for_doc_ids(results_keywords)  # histogram ids

    logger.debug(f"Filter hist: {filter_hist}")

    # numbers of matching documents
    matching_docs = list(evaluate_query(percentile, filter_hist))

    logger.debug(f"Matching docs: {matching_docs}")
    metadata = get_metadata(matching_docs)

    return {"query": percentile, "keywords": keywords, "results": metadata}


@app.post("/search2")
def search2(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords)  # document ids

    # numbers of matching documents
    matching_docs = list(evaluate_query(percentile))

    # combine the results from the two sources
    combined_results = combine_results(results_keywords, matching_docs)

    metadata = get_metadata(combined_results)

    return {"query": percentile, "keywords": keywords, "results": metadata}


@app.post("/single_query")
def search_single(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords)  # document ids

    query = parse_precentile_query(percentile)

    # numbers of matching histograms
    result = run(INDEX, [query], "index")

    matching_histograms = result[0]

    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])

    # combine the results from the two sources

    combined_results = combine_results(results_keywords, matching_docs)

    metadata = get_metadata(combined_results)

    return {"query": percentile, "keywords": keywords, "results": metadata}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8001)
