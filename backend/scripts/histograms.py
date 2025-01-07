import argparse
import json
import os
import time
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fainder.typing import Histogram
from fainder.utils import ROUNDING_PRECISION, configure_run, save_output
from loguru import logger


def parse_args() -> argparse.Namespace:
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
    parser = argparse.ArgumentParser(
        description="Compute histograms from a collection of Parquet files.",
        formatter_class=argparse.MetavarTypeHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input",
        type=lambda s: Path(os.path.expandvars(s)),
        required=True,
        help="path to Parquet dataset collection",
        metavar="SRC",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=lambda s: Path(os.path.expandvars(s)),
        required=True,
        help="path to the compressed histogram output",
        metavar="DEST",
    )
    parser.add_argument(
        "-f",
        "--scaling-factor",
        default=1,
        type=float,
        help="scaling factor to downsample or upsample the dataset (default: %(default)s)",
    )
    parser.add_argument(
        "--bin-range",
        nargs=2,
        default=None,
        type=int,
        help="range to randomly draw n_bins per histogram ('auto' if None, default: %(default)s)",
    )
    parser.add_argument(
        "--compute-frequencies",
        action="store_true",
        help="compute frequencies instead of densities",
    )
    parser.add_argument(
        "-w",
        "--workers",
        default=os.cpu_count(),
        type=int,
        help="number of worker processes (default: %(default)s)",
    )
    parser.add_argument(
        "--seed",
        default=42,
        type=int,
        help="random seed (default: %(default)s)",
    )
    parser.add_argument(
        "--log-file",
        type=lambda s: Path(os.path.expandvars(s)),
        default=Path(f"logs/hist_computation_{timestamp}.log"),
        help="path to log file (default: %(default)s)",
        metavar="LOG",
    )
    return parser.parse_args()


def compute_histogram(
    input_file: Path,
    seed: int,
    bin_range: tuple[int, int] | None,
    scaling_factor: float = 1,
    density: bool = True,
) -> tuple[list[Histogram], int, list[str]] | str:
    try:
        np.seterr(all="raise")
        hists: list[Histogram] = []
        rng = np.random.default_rng(seed)
        df = pd.read_parquet(input_file, engine="pyarrow").select_dtypes(include="number")
        bin_counter = 0
        column_names = list(df.columns)
        for _, values in df.items():
            values.dropna(inplace=True)
            # We filter out huge values to prevent overflows in the index (and since they
            # are unrealistic for percentile queries). Since multiple large integer values are
            # represented by the same float value, we cast them before counting unique values.
            values = values[(values > -(2**53)) & (values < 2**53)].astype(dtype=np.float64)
            values = values.round(ROUNDING_PRECISION)
            if values.nunique() > 1 and values.min() != values.max():
                probability = scaling_factor
                while probability > rng.random():
                    bins: int | str
                    if bin_range:
                        bins = min(
                            values.nunique() - 1,
                            rng.integers(low=bin_range[0], high=bin_range[1] + 1),
                        )
                        bin_counter += bins
                    else:
                        bins = "auto"
                    hist = np.histogram(
                        values,
                        bins=bins,
                        # Numpy computes density different than we need them
                        density=False,
                    )

                    # Histogram verification
                    assert (np.diff(hist[1]) == 0).sum() == 0
                    assert hist[1].dtype == np.float64
                    if density:
                        hist = (np.divide(hist[0], hist[0].sum(), dtype=np.float32), hist[1])
                        assert np.isclose(hist[0].sum(), 1)
                    else:
                        hist = (hist[0].astype(np.uint32, casting="safe"), hist[1])

                    hists.append(hist)

                    probability -= 1
        return hists, bin_counter, column_names
    except AssertionError as e:
        print(f"{input_file}: {e}")
        raise AssertionError(input_file) from e
    except Exception as e:
        print(f"{input_file}: {type(e)} {e}")
        return f"{input_file}: {type(e)} {e}"


def load_archive(path: Path) -> tuple[str, Histogram]:
    archive = np.load(path)
    hist = (archive["values"], archive["bins"])
    archive.close()
    return path.stem, hist


def get_indexes(metadata: dict[str, Any], file_id: str, column: str) -> tuple[int, int]:
    """
    Get the indexes of the id and column in the metadata

    Args:
    metadata: dict: metadata of the dataset
    id: str: id of the file
    column: str: column name
    """
    for i, file in enumerate(metadata["recordSet"]):
        if file["@id"] == file_id:
            for i2, field in enumerate(file["field"]):
                if field["name"] == column:
                    return i, i2
    if column != "":
        return get_indexes(metadata, file_id, "")
    raise ValueError("The id or column was not found in the metadata")


def main() -> None:
    start = time.perf_counter()
    args = parse_args()
    configure_run("INFO", args.log_file)
    logger.debug(vars(args))

    n_files = len(list(args.input.iterdir()))
    seeds = np.random.default_rng(args.seed).integers(10000, size=n_files)
    with Pool(processes=args.workers) as pool:
        fn = partial(
            compute_histogram,
            bin_range=args.bin_range,
            scaling_factor=args.scaling_factor,
            density=not args.compute_frequencies,
        )
        results = pool.starmap(fn, zip(sorted(args.input.iterdir()), seeds, strict=True))

    # histid: column name + file name
    list_of_hist: list[str] = []

    dict_file_id_to_hists: dict[int, list[int]] = {}

    dict_doc_id_to_file_ids: dict[int, list[int]] = {}

    column_names: dict[str, list[str]] = {}

    # make directory for the histograms
    os.makedirs(args.input.parent / "hists", exist_ok=True)

    # list of filenames in the input directory withouth the extension
    file_paths: list[str] = []
    for file in sorted(args.input.iterdir()):
        helper = str(file).split("/")
        file_paths.append(helper[-1].split(".")[0])

    print(file_paths)

    metadata_path = args.input.parent / "metadata"

    errors: list[str] = []
    hists: list[tuple[np.uint32, Histogram]] = []
    i = 0
    bin_counter = 0
    list_of_docs = []
    doc_id = -1

    for filenumber, result in enumerate(results):
        if isinstance(result, str):
            errors.append(result)
        else:
            spilt = file_paths[filenumber].split("&")
            name = spilt[0] + "&" + spilt[1]
            metadata = None
            with open(metadata_path / f"{name}.json") as f:
                metadata = json.load(f)
            dataset_name = spilt[1]
            if dataset_name not in list_of_docs:
                doc_id = doc_id + 1
                list_of_docs.append(dataset_name)
                dict_doc_id_to_file_ids[doc_id] = [filenumber]
            else:
                dict_doc_id_to_file_ids[doc_id].append(filenumber)

            for histnumber, hist in enumerate(result[0]):
                hists.append((np.uint32(i), hist))
                column_name = result[2][histnumber]

                # get the indexes of the id and column in the metadata
                file_id, id2 = get_indexes(metadata, spilt[2] + ".csv", column_name)

                h = {
                    "histogram": hist[0].tolist(),
                    "bin_edges": hist[1].tolist(),
                }

                metadata["recordSet"][file_id]["field"][id2]["histogram"] = h

                filepath = file_paths[filenumber]
                histid = f"{column_name}&{filepath}"
                list_of_hist.append(histid)

                if column_name in column_names:
                    column_names[column_name].append(histid)
                else:
                    column_names[column_name] = [histid]

                # add the histogram id to dict_doc_id_to_hists

                if filenumber in dict_file_id_to_hists:
                    dict_file_id_to_hists[filenumber].append(i)
                else:
                    dict_file_id_to_hists[filenumber] = [i]
                i += 1

            with open(metadata_path / f"{name}.json", "w") as f:
                json.dump(metadata, f)

            bin_counter += result[1]

    metadata = {
        "list_of_hist": list_of_hist,
        "column_names": column_names,
        "list_of_docs": list_of_docs,
        "dict_file_id_to_hists": dict_file_id_to_hists,
        "dict_doc_id_to_file_ids": dict_doc_id_to_file_ids,
    }

    # save in parent of input directory

    json_path = args.input.parent / "metadata.json"
    with open(json_path, "w") as f:
        json.dump(metadata, f)

    save_output(args.output, hists, name="histograms")

    end = time.perf_counter()
    logger.info(
        f"Parsed {n_files} files and generated {i} histograms with a total of {bin_counter} bins "
        f"in {end - start:.2f}s with {len(errors)} errors."
    )
    logger.trace(f"histogram_count, {i}")
    logger.trace(f"bin_count, {bin_counter}")
    logger.trace(f"error_count, {len(errors)}")
    logger.trace(f"construction_time, {end - start}")
    for error in errors:
        logger.debug(error)


if __name__ == "__main__":
    main()
