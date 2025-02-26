from collections.abc import Sequence

import grpc
from loguru import logger

from backend.config import IndexingError
from backend.proto.lucene_connector_pb2 import (  # type: ignore
    QueryRequest,
    RecreateIndexRequest,
)
from backend.proto.lucene_connector_pb2_grpc import LuceneConnectorStub


class LuceneConnector:
    channel: grpc.Channel | None

    def __init__(self, host: str, port: str) -> None:
        self.host = host
        self.port = port
        self.channel = None

    def __del__(self) -> None:
        self.close()

    def connect(self) -> None:
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.stub = LuceneConnectorStub(self.channel)
        logger.debug(f"Connected to Lucene server with host: {self.host} and port: {self.port}")

    def close(self) -> None:
        if self.channel:
            self.channel.close()
            self.channel = None
            logger.debug("gRPC channel closed")

    def evaluate_query(
        self, query: str, doc_ids: set[int] | None = None, enable_highlighting: bool = False
    ) -> tuple[Sequence[int], Sequence[float], dict[int, dict[str, str]]]:
        """
        Evaluates a keyword query using the Lucene server.

        For large result sets that exceed gRPC message size limits, this method
        will automatically fall back to streaming.

        Args:
            query: The query string to be evaluated by Lucene.
            doc_ids: A set of document IDs to consider as a filter (none by default).
            enable_highlighting: Whether to enable highlighting (default False).

        Returns:
            list[int]: A list of document IDs that match the query.
            list[float]: A list of scores for each document ID.
            dict[int, dict[str, str]]: A dictionary mapping document IDs to dictionaries
                                      of field names to highlighted snippets.
        """
        if not self.channel:
            self.connect()

        try:
            logger.debug(f"Executing query: '{query}' with filter: {doc_ids}")
            # Clear any previous state by creating a fresh request
            request = QueryRequest(
                query=query, doc_ids=doc_ids or [], enable_highlighting=enable_highlighting
            )

            try:
                # First try the regular method
                response = self.stub.Evaluate(request)

                # Create a fresh highlights dict for each query result
                highlights: dict[int, dict[str, str]] = {}

                # Only process highlights if enabled
                if enable_highlighting:
                    for doc_id in response.results:
                        if doc_id in response.highlights:
                            highlights[doc_id] = dict(response.highlights[doc_id].fields)

                return response.results, response.scores, highlights

            except grpc.RpcError as e:
                # Check if this is a message size error
                if "Received message larger than max" in str(e):
                    logger.info("Keyword query result too large, falling back to streaming")
                    return self.evaluate_query_stream(query, doc_ids, enable_highlighting)
                # Re-raise if it's not a message size error
                raise e

        except grpc.RpcError as e:
            logger.error(f"Calling Lucene raised an error: {e}")
            return [], [], {}

    def evaluate_query_stream(
        self, query: str, doc_ids: set[int] | None = None, enable_highlighting: bool = False
    ) -> tuple[Sequence[int], Sequence[float], dict[int, dict[str, str]]]:
        """
        Evaluates a keyword query using the Lucene server with streaming for large result sets.

        This method uses gRPC streaming to handle large responses that would exceed
        message size limits.

        Args:
            query: The query string to be evaluated by Lucene.
            doc_ids: A set of document IDs to consider as a filter (none by default).
            enable_highlighting: Whether to enable highlighting (default True).

        Returns:
            list[int]: A list of document IDs that match the query.
            list[float]: A list of scores for each document ID.
            dict[int, dict[str, str]]: A dictionary mapping document IDs to dictionaries
                                      of field names to highlighted snippets.
        """
        if not self.channel:
            self.connect()

        try:
            logger.debug(f"Executing streaming query: '{query}' with filter: {doc_ids}")

            # Create the request - same as for non-streaming
            request = QueryRequest(
                query=query, doc_ids=doc_ids or [], enable_highlighting=enable_highlighting
            )

            # Collect results from all chunks
            all_doc_ids: list[int] = []
            all_scores: list[float] = []
            highlights: dict[int, dict[str, str]] = {}

            # Process the streaming response
            for chunk in self.stub.EvaluateStream(request):
                # Add documents from this chunk to our result lists
                all_doc_ids.extend([int(doc_id) for doc_id in chunk.doc_ids])
                all_scores.extend([float(score) for score in chunk.scores])

                # Process highlights if available
                if enable_highlighting:
                    for doc_id in chunk.highlights:
                        highlights[doc_id] = dict(chunk.highlights[doc_id].fields)

                # Optionally log progress
                logger.debug(f"Received chunk with {len(chunk.doc_ids)} documents")

            return all_doc_ids, all_scores, highlights

        except grpc.RpcError as e:
            logger.error(f"Streaming query to Lucene raised an error: {e}")
            return [], [], {}

    async def recreate_index(self) -> None:
        """Triggers the recreation of the Lucene index on the server side."""
        if not self.channel:
            self.connect()

        try:
            response = self.stub.RecreateIndex(RecreateIndexRequest())
            if not response.success:
                raise IndexingError(f"Failed to recreate Lucene index: {response.message}")
            logger.info("Lucene index recreation completed")
        except grpc.RpcError as e:
            logger.error(f"Lucene index recreation failed: {e}")
            raise IndexingError("Failed to recreate Lucene index") from e
