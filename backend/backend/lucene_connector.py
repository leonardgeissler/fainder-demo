import time
from collections.abc import Sequence

import grpc
from loguru import logger

from backend.proto.keyword_query_pb2 import QueryRequest  # type: ignore
from backend.proto.keyword_query_pb2_grpc import KeywordQueryStub


class LuceneConnector:
    def __init__(self, host: str, port: str) -> None:
        self.host = host
        self.port = port
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = KeywordQueryStub(self.channel)

        logger.debug(f"Lucene connector initialized with host: {host} and port: {port}")

    def __del__(self) -> None:
        self.channel.close()
        logger.debug("gRPC channel closed")

    def evaluate_query(
        self, query: str, doc_ids: list[int] | None = None
    ) -> tuple[Sequence[int], Sequence[float]]:
        """
        Evaluates a keyword query using the Lucene server.

        Args:
            query: The query string to be evaluated by Lucene.
            doc_ids: A list of document IDs to consider as a filter (none by default).

        Returns:
            list[int]: A list of document IDs that match the query.
            list[float]: A list of scores for each document ID.
        """
        # TODO: Use the ranking returned by Lucene to sort the results
        start = time.perf_counter()
        try:
            logger.debug(f"Executing query: '{query}' with filter: {doc_ids}")
            response = self.stub.Evaluate(QueryRequest(query=query, doc_ids=doc_ids or []))
            result = response.results

            logger.debug(f"Lucene query execution took {time.perf_counter() - start:.3f} seconds")
            logger.debug(f"Keyword query result: {result}")

            return response.results, response.scores
        except grpc.RpcError as e:
            logger.error(f"Calling Lucene raised an error: {e}")
            return [], []
