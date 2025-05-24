from pathlib import Path
from typing import TYPE_CHECKING

import hnswlib
import numpy as np
from loguru import logger
from numpy import uint32
from sentence_transformers import SentenceTransformer

from backend.config import ColumnArray, ColumnSearchError, Metadata

if TYPE_CHECKING:
    from collections.abc import Callable

    from numpy.typing import NDArray


class HnswIndex:
    def __init__(
        self,
        path: Path,
        metadata: Metadata,
        model: str = "sentence-transformers/all-MiniLM-L6-v2",
        use_embeddings: bool = True,
        ef: int = 50,
    ) -> None:
        self.name_to_vector = metadata.name_to_vector
        self.vector_to_name = [""] * len(self.name_to_vector)
        for name, vector in self.name_to_vector.items():
            self.vector_to_name[vector] = name
        self.vector_to_cols = metadata.vector_to_cols
        self.use_embeddings = use_embeddings
        self.embedder: SentenceTransformer | None = None

        if not use_embeddings:
            logger.debug("Not loading SentenceTransformer model")
            return

        # Embedding model
        logger.debug("Loading SentenceTransformer model '{}'", model)
        self.embedder = SentenceTransformer(
            model_name_or_path=model,
            cache_folder=(path.parent / "model_cache").as_posix(),
            # Possibly use ONNX, see: https://github.com/lbhm/fainder-demo/issues/102
            # backend="onnx",
            # model_kwargs={"file_name": "onnx/model_O2.onnx"},
        )
        dimension = self.embedder.get_sentence_embedding_dimension()
        if dimension is None:
            raise ValueError("Dimension of the model is not known, cannot initialize HNSW index")
        self.dimension = dimension
        logger.debug("Model loaded")

        # HNSW index
        logger.debug("Loading HNSW index")
        self.ef = ef
        self.index = hnswlib.Index(space="cosine", dim=self.dimension)
        self.index.load_index(str(path))
        self.index.set_ef(self.ef)
        logger.debug("HNSW index loaded")

    def update(self, path: Path, metadata: Metadata) -> None:
        self.name_to_vector = metadata.name_to_vector
        self.vector_to_name = [""] * len(self.name_to_vector)
        for name, vector in self.name_to_vector.items():
            self.vector_to_name[vector] = name
        self.vector_to_cols = metadata.vector_to_cols

        if not self.use_embeddings:
            return

        self.index = hnswlib.Index(space="cosine", dim=self.dimension)
        self.index.load_index(str(path))
        self.index.set_ef(self.ef)

    def search(self, column_name: str, k: int, column_filter: set[uint32] | None) -> ColumnArray:
        if k < 0:
            raise ColumnSearchError(f"k must be a non-negative integer: {k}")

        result: set[uint32] = set()
        if k == 0:
            # Exact search
            vector_id = self.name_to_vector.get(column_name, None)
            if vector_id:
                col_ids = self.vector_to_cols.get(vector_id, set())
                result |= {uint32(col_id) for col_id in col_ids}
        else:
            if self.embedder is None:
                raise ColumnSearchError("Embedding model is not available for approximate search")

            # Nearest neighbor search
            embedding: NDArray[np.float32] = self.embedder.encode(  # pyright: ignore
                column_name, convert_to_numpy=True, normalize_embeddings=True
            )

            if column_name in self.name_to_vector:
                # If the column name exists in the index, it will be returned as the first result
                k += 1
            filter_fn: Callable[[int], bool] | None = (
                (lambda id_: id_ in column_filter) if column_filter else None
            )
            vector_ids, distances = self.index.knn_query(embedding, k=k, filter=filter_fn)
            result |= {
                uint32(col_id)
                for vector_id in vector_ids[0]
                for col_id in self.vector_to_cols[vector_id]
            }
            logger.debug(
                "Column search '{}' with k={} returned neighbors {} with distances {}",
                column_name,
                k,
                [self.vector_to_name[vector_id] for vector_id in vector_ids[0]],
                distances[0],
            )

        return np.array(list(result), dtype=np.uint32)
