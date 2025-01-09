from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from backend.search import query_bit_array, search, search_basic, search_combined, search_single

app = FastAPI()
origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# TODO: configure logging
# potentially move from loguru to standard logging to be compatible with FastAPI


@app.post("/query_bit")
def query(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    logger.info(f"Query: {percentile}")
    bit_array = query_bit_array(percentile)
    return {"query": percentile, "bit_array": bit_array}


@app.post("/search")
def search_base(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    logger.info(f"Query: {percentile} and keywords: {keywords}")
    results = search_basic(percentile, keywords)
    return {"query": percentile, "keywords": keywords, "results": results}


@app.post("/search2")
def search2(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    logger.info(f"Query: {percentile} and keywords: {keywords}")
    results = search_combined(percentile, keywords)
    return {"query": percentile, "keywords": keywords, "results": results}


@app.post("/single_query")
def search_single_query(payload: dict[str, Any]) -> dict[str, Any]:
    percentile = payload["percentile"]
    keywords = payload["keywords"]
    logger.info(f"Query: {percentile} and keywords: {keywords}")
    results = search_single(percentile, keywords)
    return {"query": percentile, "keywords": keywords, "results": results}


@app.post("/query")
def full_search(payload: dict[str, Any]) -> dict[str, Any]:
    query = payload["query"]
    logger.info(f"Query: {query}")
    results = search(query)
    return {"query": query, "results": results}
