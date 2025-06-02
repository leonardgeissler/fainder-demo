import csv
from pathlib import Path
import time

from backend.indices.percentile_op import FainderIndex
from backend.config import Settings, FainderMode

thresholds = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000]

percentiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

fainder_modes: list[FainderMode] = [FainderMode.FULL_RECALL, FainderMode.EXACT]


def log_performance_csv(
    csv_path: Path,
    threshold: int,
    percentile: float,
    result_size: int,
    fainder_mode: str,
):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with csv_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([timestamp, fainder_mode, threshold, percentile, result_size])


def run():
    logs_dir = Path("logs/fainder_results_size")
    logs_dir.mkdir(exist_ok=True)
    log_file = logs_dir / f"fainder_results_size_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    with log_file.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            ["timestamp", "fainder_mode", "threshold", "percentile", "result_size"]
        )
    settings = Settings()  # type: ignore # uses the environment variables
    fainder_index = FainderIndex(
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
        parallel=settings.fainder_parallel,
        num_workers=settings.max_workers,
        contiguous=settings.fainder_contiguous_chunks,
    )

    for threshold in thresholds:
        for percentile in percentiles:
            for fainder_mode in fainder_modes:
                result_size = len(
                    fainder_index.search(
                        percentile=percentile,
                        comparison="le",
                        reference=threshold,
                        fainder_mode=fainder_mode,
                    )
                )
                log_performance_csv(
                    csv_path=log_file,
                    threshold=threshold,
                    percentile=percentile,
                    result_size=result_size,
                    fainder_mode=fainder_mode,
                )
                print(
                    f"Threshold: {threshold}, Percentile: {percentile}, Fainder Mode: {fainder_mode}, Result Size: {result_size}"
                )


if __name__ == "__main__":
    run()
