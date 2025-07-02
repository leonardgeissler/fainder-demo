import argparse
import os
import sys
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import hnswlib
import numpy as np
import tantivy
from fainder.execution.parallel_processing import FainderChunkLayout, partition_histogram_ids
from fainder.preprocessing.clustering import cluster_histograms
from fainder.preprocessing.percentile_index import create_index
from fainder.typing import Histogram
from fainder.utils import configure_run, save_output
from loguru import logger
from pydantic import DirectoryPath
from sentence_transformers import SentenceTransformer

from backend.config import Settings
from backend.indices import TantivyIndex, get_tantivy_schema
from backend.utils import dump_json, load_json

if TYPE_CHECKING:
    from numpy.typing import NDArray


def _prepare_document_for_tantivy(json_doc: dict[str, Any]) -> None:
    """Modify the document to be ingested by Tantivy."""
    if "keywords" in json_doc:
        keywords: list[Any] = json_doc["keywords"]
        json_doc["keywords"] = "; ".join(str(keyword) for keyword in keywords)

    if (
        "creator" in json_doc
        and isinstance(json_doc["creator"], dict)
        and "name" in json_doc["creator"]
    ):
        json_doc["creator"] = json_doc["creator"]["name"]

    if (
        "publisher" in json_doc
        and isinstance(json_doc["publisher"], dict)
        and "name" in json_doc["publisher"]
    ):
        json_doc["publisher"] = json_doc["publisher"]["name"]


def generate_metadata(
    croissant_path: DirectoryPath,
    metadata_path: DirectoryPath,
    tantivy_path: DirectoryPath,
    return_documents: bool = True,
) -> tuple[
    list[tuple[np.uint32, Histogram]], dict[str, int], dict[int, dict[str, Any]], TantivyIndex
]:
    """Load Croissant files and generate metadata.

    While loading the files, assign unique IDs to documents, columns, histograms, and vectors.
    This function also creates and stores mappings between entities that are needed for
    downstream processing.
    """
    # Initialize mappings
    # NOTE: We need the vector_id intermediate step because hnswlib requires int IDs for vectors
    doc_to_cols: list[list[int]] = []
    doc_to_path: list[str] = []
    name_to_vector: dict[str, int] = {}
    vector_to_cols: dict[int, set[int]] = defaultdict(set)

    json_docs: dict[int, dict[str, Any]] = {}
    tantivy_docs: list[tantivy.Document] = []
    tantivy_schema = get_tantivy_schema()

    # First pass: count the number of histograms
    logger.info("Counting histograms")
    num_hists = 0
    num_cols = 0
    for path in croissant_path.iterdir():
        json_doc = load_json(path)
        doc_to_cols.append([])
        for record_set in json_doc.get("recordSet", []):
            fields = record_set.get("field", [])
            num_hists += sum(1 for col in fields if "histogram" in col)
            num_cols += len(fields)

    logger.info("Found {} histograms and {} columns", num_hists, num_cols)

    # We need to pre-allocate the column ID mapping since we insert at different indices
    col_to_doc: list[int] = [-1] * num_cols

    # Second pass: process the documents with the updated column IDs
    logger.info("Processing documents")
    hists: list[tuple[np.uint32, Histogram]] = []
    vector_id = 0
    col_id_hist = 0
    col_id_no_hist = num_hists

    for doc_id, path in enumerate(sorted(croissant_path.iterdir())):
        # Read the file and add a document ID to it
        json_doc = load_json(path)
        json_doc["id"] = doc_id
        if return_documents:
            json_docs[doc_id] = json_doc

        # Ingest histograms and assign unique ids to columns
        try:
            for record_set in json_doc["recordSet"]:
                for col in record_set["field"]:
                    if "histogram" in col:
                        col_id = col_id_hist
                        densities = np.array(col["histogram"]["densities"], dtype=np.float32)
                        bins = np.array(col["histogram"]["bins"], dtype=np.float64)
                        hists.append((np.uint32(col_id_hist), (densities, bins)))
                        col["histogram"]["id"] = col_id_hist
                        col_id_hist += 1
                    else:
                        col_id = col_id_no_hist
                        col_id_no_hist += 1

                    col["id"] = col_id
                    doc_to_cols[doc_id].append(col_id)
                    col_to_doc[col_id] = doc_id

                    col_name = col["name"]
                    if col_name not in name_to_vector:
                        name_to_vector[col_name] = vector_id
                        vector_id += 1
                    vector_to_cols[name_to_vector[col_name]].add(col_id)
        except KeyError as e:
            logger.error("KeyError {} reading file {}", e, path)

        # Store the document path for file-based Croissant stores
        doc_to_path.append(path.name)

        # Replace the original file with the extended document
        dump_json(json_doc, path)

        # Prepare document for Tantivy indexing
        _prepare_document_for_tantivy(json_doc)
        tantivy_docs.append(tantivy.Document.from_dict(json_doc, tantivy_schema))  # pyright: ignore[reportUnknownMemberType]

    logger.info(
        "Found {} documents with {} columns and {} histograms.",
        len(doc_to_cols),
        len(col_to_doc),
        num_hists,
    )

    # Index the documents in Tantivy (we index all documents at once to increase performance)
    logger.info("Initializing Tantivy index")
    tantivy_index = TantivyIndex(tantivy_path, recreate=True)
    tantivy_index.add_documents(tantivy_docs)

    # Save the mappings and indices
    logger.info("Saving metadata")
    dump_json(
        {
            "doc_to_cols": doc_to_cols,
            "doc_to_path": doc_to_path,
            "col_to_doc": col_to_doc,
            "num_hists": num_hists,
            "name_to_vector": name_to_vector,
            "vector_to_cols": {str(k): list(v) for k, v in vector_to_cols.items()},
        },
        metadata_path,
    )

    return hists, name_to_vector, json_docs, tantivy_index


def generate_fainder_indices(
    hists: Sequence[tuple[int | np.integer[Any], Histogram]],
    output_path: Path,
    config_name: str = "default",
    n_clusters: int = 27,
    bin_budget: int = 270,
    alpha: float = 1,
    transform: Literal["standard", "robust", "quantile", "power"] | None = None,
    algorithm: Literal["agglomerative", "hdbscan", "kmeans"] = "kmeans",
    seed: int = 42,
    workers: int | None = os.cpu_count(),
) -> None:
    logger.info(f"Starting Fainder index generation with config '{config_name}'")

    logger.info("Clustering {} histograms", len(hists))
    clustered_hists, cluster_bins, _ = cluster_histograms(
        hists=hists,
        transform=transform,
        quantile_range=(0.25, 0.75),
        algorithm=algorithm,
        n_cluster_range=(n_clusters, n_clusters),
        n_global_bins=bin_budget,
        alpha=alpha,
        seed=seed,
        workers=workers,
        verbose=False,
    )

    logger.info("Creating rebinning index")
    rebinning_index, _, _ = create_index(
        clustered_hists=clustered_hists,
        cluster_bins=cluster_bins,
        index_method="rebinning",
        workers=workers,
    )

    logger.info("Creating conversion index")
    conversion_index, _, _ = create_index(
        clustered_hists=clustered_hists,
        cluster_bins=cluster_bins,
        index_method="conversion",
        workers=workers,
    )

    # Save indices with config name in the filename
    rebinning_file = f"{config_name}_rebinning.zst"
    conversion_file = f"{config_name}_conversion.zst"

    save_output(
        output_path / rebinning_file,
        (rebinning_index, cluster_bins),
        name=f"rebinning index ({config_name})",
    )
    save_output(
        output_path / conversion_file,
        (conversion_index, cluster_bins),
        name=f"conversion index ({config_name})",
    )

    # Save or update the config information in a JSON file
    config_file = output_path / "configs.json"
    configs = {}
    if config_file.exists():
        configs = load_json(config_file)

    # Add or update the config
    configs[config_name] = {
        "n_clusters": n_clusters,
        "bin_budget": bin_budget,
        "alpha": alpha,
        "transform": transform,
        "algorithm": algorithm,
        "rebinning_file": rebinning_file,
        "conversion_file": conversion_file,
    }

    dump_json(configs, config_file)

    # For the default config, also save with the default filenames for backward compatibility
    if config_name == "default":
        save_output(
            output_path / "rebinning.zst", (rebinning_index, cluster_bins), name="rebinning index"
        )
        save_output(
            output_path / "conversion.zst",
            (conversion_index, cluster_bins),
            name="conversion index",
        )

    save_output(output_path / "histograms.zst", hists, name="histograms")


def generate_embedding_index(
    name_to_vector: dict[str, int],
    output_path: Path,
    model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    batch_size: int = 32,
    show_progress_bar: bool = True,
    precision: Literal["float32", "int8", "uint8", "binary", "ubinary"] = "float32",
    normalize_embeddings: bool = True,
    ef_construction: int = 400,
    n_bidirectional_links: int = 64,
    seed: int = 42,
) -> None:
    strings = list(name_to_vector.keys())
    ids = np.array(list(name_to_vector.values()), dtype=np.uint64)

    logger.info("Generating embeddings")
    embedder = SentenceTransformer(
        model_name_or_path=model_name,
        cache_folder=(output_path / "model_cache").as_posix(),
        # Possibly use ONNX, see: https://github.com/lbhm/fainder-demo/issues/102
        # backend="onnx",
        # model_kwargs={"file_name": "onnx/model_O2.onnx"},
    )
    # Maybe remove the module compilation if it does not help with performance
    embedder.compile()  # type: ignore[no-untyped-call]
    embeddings: NDArray[np.float32] = embedder.encode(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        sentences=strings,
        batch_size=batch_size,
        show_progress_bar=show_progress_bar,
        convert_to_numpy=True,
        precision=precision,
        normalize_embeddings=normalize_embeddings,
    )

    logger.info("Creating HNSW index")
    index = hnswlib.Index(space="cosine", dim=embeddings.shape[1])
    index.init_index(
        max_elements=embeddings.shape[0],
        ef_construction=ef_construction,
        M=n_bidirectional_links,
        random_seed=seed,
    )
    index.add_items(embeddings, ids)

    logger.info("Saving HNSW index")
    index.save_index((output_path / "index.bin").as_posix())


def save_histograms_parallel(
    hists: Sequence[tuple[int | np.integer[Any], Histogram]],
    output_path: Path,
    n_chunks: int,
    chunk_layout: FainderChunkLayout = FainderChunkLayout.ROUND_ROBIN,
) -> None:
    """Save histograms in parallel chunks for Fainder."""
    logger.info("Partitioning histogram IDs for parallel processing with {} chunks", n_chunks)
    if n_chunks <= 0:
        raise ValueError("Number of chunks must be greater than 0")
    hist_id_chunks = partition_histogram_ids(
        [int(id_) for id_, _ in hists], num_partitions=n_chunks, chunk_layout=chunk_layout
    )
    logger.info(
        "Partitioned histogram IDs into {} chunks of length {}",
        len(hist_id_chunks),
        len(hist_id_chunks[0]) if hist_id_chunks else 0,
    )

    # Create directory for split histograms
    if chunk_layout == FainderChunkLayout.CONTIGUOUS:
        split_dir = output_path / f"histograms_split_contiguous_{n_chunks}"
    else:
        split_dir = output_path / f"histograms_split_round_robin_{n_chunks}"

    split_dir.mkdir(exist_ok=True, parents=True)
    logger.info(f"Created directory for split histograms: {split_dir}")

    for i in range(n_chunks):
        logger.info("Saving histograms for chunk {}", i)
        # split up the histograms into chunks for each worker
        chunk_hists = [(id_, hist) for id_, hist in hists if id_ in hist_id_chunks[i]]
        logger.info("Chunk {} will process {} histograms", i, len(chunk_hists))
        save_output(
            split_dir / f"histograms_{i}.zst",
            chunk_hists,
            name="histograms",
        )
        logger.info(
            "Saved {} histograms to file: {}", len(chunk_hists), split_dir / f"histograms_{i}.zst"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate metadata and indices for a collection of dataset profiles"
    )
    parser.add_argument(
        "--no-fainder", action="store_true", help="Skip generating Fainder indices"
    )
    parser.add_argument(
        "--no-embeddings", action="store_true", help="Skip generating embedding index"
    )
    parser.add_argument(
        "--no-hists-parallel",
        action="store_true",
        help="Do not save histograms in parallel chunks for Fainder",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        type=str,
        choices=["DEBUG", "INFO", "WARNING"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--config-name",
        default="default",
        type=str,
        help="Configuration name for the Fainder indices",
    )
    parser.add_argument(
        "--multi-configs",
        nargs="+",
        type=str,
        help=(
            "List of multiple configurations in format 'name:clusters:budget'. "
            "Example: --multi-configs default:27:270 small:10:100"
        ),
    )
    parser.add_argument(
        "--multi-chunks",
        nargs="+",
        type=str,
        help=(
            "List of multiple chunk configurations in format 'chunks:layout' "
            "where layout is either 'contiguous' or 'round_robin'. "
            "Example: --multi-chunks 4:contiguous 8:round_robin"
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    try:
        settings = Settings()  # type: ignore[call-arg]
        if args.log_level is None:
            configure_run(settings.log_level)
        else:
            configure_run(args.log_level)
        logger.debug(settings.model_dump())
    except Exception as e:  # noqa: BLE001
        logger.error("Error loading settings: {}", e)
        sys.exit(1)

    hists, name_to_vector, _, _ = generate_metadata(  # type: ignore[assignment]
        settings.croissant_path,
        settings.metadata_path,
        settings.tantivy_path,
        return_documents=False,
    )

    if not args.no_fainder:
        # Handle multiple configurations if specified
        if args.multi_configs:
            for config_str in args.multi_configs:
                try:
                    parts = config_str.split(":")
                    if len(parts) != 3:  # noqa: PLR2004
                        logger.warning(
                            f"Invalid configuration format: {config_str}, skipping. "
                            f"Expected format: name:clusters:budget"
                        )
                        continue

                    config_name = str(parts[0])
                    n_clusters = int(parts[1])
                    bin_budget = int(parts[2])

                    logger.info(
                        f"Generating additional Fainder indices with config "
                        f"'{config_name}', clusters={n_clusters}, budget={bin_budget}"
                    )
                    generate_fainder_indices(
                        hists=hists,
                        output_path=settings.fainder_path,
                        config_name=config_name,
                        n_clusters=n_clusters,
                        bin_budget=bin_budget,
                        alpha=settings.fainder_alpha,
                        transform=settings.fainder_transform,
                        algorithm=settings.fainder_cluster_algorithm,
                    )
                except ValueError as e:
                    logger.error(f"Error processing configuration {config_str}: {e}")
                    continue

        else:
            # Handle single configuration
            generate_fainder_indices(
                hists=hists,
                output_path=settings.fainder_path,
                config_name=args.config_name,
                n_clusters=settings.fainder_n_clusters,
                bin_budget=settings.fainder_bin_budget,
                alpha=settings.fainder_alpha,
                transform=settings.fainder_transform,
                algorithm=settings.fainder_cluster_algorithm,
            )

    if not args.no_embeddings:
        generate_embedding_index(
            name_to_vector=name_to_vector,
            output_path=settings.embedding_path,
            model_name=settings.embedding_model,
            batch_size=settings.embedding_batch_size,
            ef_construction=settings.hnsw_ef_construction,
            n_bidirectional_links=settings.hnsw_n_bidirectional_links,
        )

    if not args.no_hists_parallel:
        # Process multiple chunk configurations if specified
        if args.multi_chunks:
            for chunk_str in args.multi_chunks:
                try:
                    parts = chunk_str.split(":")
                    if len(parts) != 2:  # noqa: PLR2004
                        logger.warning(
                            f"Invalid chunk configuration format: {chunk_str}, skipping. "
                            f"Expected format: chunks:layout"
                        )
                        continue

                    n_chunks = int(parts[0])
                    layout = str(parts[1]).strip().upper()
                    try:
                        chunk_layout = FainderChunkLayout(layout)
                    except ValueError:
                        logger.warning(
                            f"Invalid layout '{layout}' in chunk configuration: "
                            f"{chunk_str}, skipping. Expected: 'contiguous' or 'round_robin'"
                        )
                        continue

                    logger.info(
                        f"Saving histograms with chunk configuration: "
                        f"chunks={n_chunks}, layout={layout}"
                    )
                    save_histograms_parallel(
                        hists,
                        output_path=settings.fainder_path,
                        n_chunks=n_chunks,
                        chunk_layout=chunk_layout,
                    )
                except ValueError as e:
                    logger.error(f"Error processing chunk configuration {chunk_str}: {e}")
                    continue
        else:
            # Default single chunk configuration
            save_histograms_parallel(
                hists,
                output_path=settings.fainder_path,
                n_chunks=settings.fainder_num_chunks,
                chunk_layout=settings.fainder_chunk_layout,
            )
