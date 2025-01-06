import time
from fastapi import FastAPI
import os
import json

import requests
from fainder.execution.runner import run
from fainder.typing import PercentileQuery

from fainder_demo.percentile_grammar import evaluate_query, number_of_matching_histograms_to_doc_number

from typing import Any, Literal

from fastapi.middleware.cors import CORSMiddleware

from loguru import logger

from fainder_demo.config import PATH_TO_METADATA, INDEX, LIST_OF_DOCS, DICT_FILE_ID_TO_HISTS  

from numpy import uint32



app = FastAPI()
origins = [
    "http://localhost:3000",  # Frontend development server
    "http://127.0.0.1:3000"   # Alternate localhost
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

    
    return {
        "query": percentile,
        "bit_array": bit_array
    }

def call_lucene_server(keywords: str) -> list[int]:
    start = time.perf_counter()

    # POST request to lucene server at port 8001 
    request = "http://localhost:8001/search"
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(
            request, 
            json={"keywords": keywords},
            headers=headers
        )
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        
        logger.debug(f"Raw Lucene response: {response.text}")
        array: list[int] = data["results"]

        # verify is array of integers
        assert all(isinstance(x, int) for x in array)

        logger.info(f"Lucene server took {time.perf_counter() - start} seconds")
        logger.debug(f"Lucene results: {array}")

        return array
    except Exception as e:
        logger.error(f"Error calling Lucene server: {e}")
        logger.error(f"Response content: {response.text if 'response' in locals() else 'No response'}")
        return []

def get_metadata(doc_ids: list[int]) -> list[dict[str, Any]]:

    list_of_metadata_files = os.listdir(PATH_TO_METADATA)

    metadata = []

    for i in doc_ids:
        metadata_file = list_of_metadata_files[i]
        metadata.append(json.load(open(os.path.join(PATH_TO_METADATA, metadata_file))))

    return metadata

def combine_results(lucene_results: list[int], fainder_results: list[int]) -> list[int]:

    print(f"Lucene results: {lucene_results}")
    print(f"Fainder results: {fainder_results}")
    
    result = []

    for i in lucene_results:
        if i in fainder_results:
            result.append(i)

    return result


def get_hists_for_doc_ids(doc_ids: list[int]) -> set[uint32]:

    hists_int = []

    for i in doc_ids:
        try:
            hists_int.extend(DICT_FILE_ID_TO_HISTS[str(i)])
        except KeyError:
            pass


    hists = set(uint32(i) for i in hists_int)

    return hists

@app.post("/search")
def search(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords) # document ids

    if len(results_keywords) == 0:
        return {
            "query": percentile,
            "keywords": keywords,
            "results": []
        }

    filter_hist = get_hists_for_doc_ids(results_keywords) # histogram ids

    logger.debug(f"Filter hist: {filter_hist}")

    # numbers of matching documents
    matching_docs = list(evaluate_query(percentile, filter_hist))

    logger.debug(f"Matching docs: {matching_docs}")
    metadata = get_metadata(matching_docs)

    return {
        "query": percentile,
        "keywords": keywords,
        "results": metadata
    }


@app.post("/search2")
def search2(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords) # document ids

    # numbers of matching documents
    matching_docs = list(evaluate_query(percentile))

    # combine the results from the two sources
    combined_results = combine_results(results_keywords, matching_docs)


    metadata = get_metadata(combined_results)


    return {
        "query": percentile,
        "keywords": keywords,
        "results": metadata
    }

@app.post("/single_query")
def search_single(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    print(f"Query: {percentile} and keywords: {keywords}")

    results_keywords = call_lucene_server(keywords) # document ids

    query = parse_precentile_query(percentile)

    # numbers of matching histograms
    result = run(INDEX, [query], "index")

    matching_histograms = result[0]

    matching_docs = number_of_matching_histograms_to_doc_number(matching_histograms[0])

    # combine the results from the two sources

    combined_results = combine_results(results_keywords, matching_docs)

    metadata = get_metadata(combined_results)


    return {
        "query": percentile,
        "keywords": keywords,
        "results": metadata
    }



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)



