# runs keywords and loggs the number of results for each keyword
import json
import time
from backend.config import Metadata
from backend.engine.engine import Engine
from backend.indices.percentile_op import FainderIndex
from backend.indices.keyword_op import TantivyIndex
from backend.indices.name_op import HnswIndex as ColumnIndex
from backend.config import ExecutorType, Settings
from pathlib import Path
import csv


KEYWORDS = [
    "age",
    "date",
    "name",
    "address",
    "city",
    "state",
    "zip",
    "phone",
    "email",
    "web",
    "germany",
    "the",
    "a",
    "test",
    "born",
    "by",
    "blood",
    "heart",
    "lung",
    "italy",
    "usa",
    "bank",
    "health",
    "money",
    "school",
    "work",
    "music",
    "family",
    "food",
    "water",
    "time",
    "company",
    "xylophone",
    "zebra",
    "yacht",
    "quantum",
    "zeppelin",
    "unicorn",
    "volcano",
    "ninja",
    "dragon",
    "wizard",
    "vampire",
    "zombie",
    "pirate",
    "dinosaur",
    "spaceship",
    "a*",
    "b*",
    "c*",
    "d*",
    "e*",
    "f*",
    "g*",
    "h*",
    "i*",
    "j*",
    "k*",
    "l*",
    "m*",
    "n*",
    "o*",
    "p*",
    "q*",
    "r*",
    "s*",
    "t*",
    "u*",
    "v*",
    "w*",
    "x*",
    "y*",
    "z*",
]

def setup_engine() -> Engine:

    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    fainder_index = FainderIndex(
        rebinning_path=None,
        conversion_path=None,
        histogram_path=None,
    )
    column_index = ColumnIndex(path=settings.hnsw_index_path, metadata=metadata)
    return Engine(
            tantivy_index=TantivyIndex(
                index_path=str(settings.tantivy_path), recreate=False
            ),
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=0,
            executor_type=ExecutorType.SIMPLE,
        )

def setup_logging() -> Path:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    number_of_results_log_path = Path("logs")/"number_of_results"

    number_of_results_log_path.mkdir(parents=True, exist_ok=True)

    number_of_results_log_path = number_of_results_log_path / f"number_of_results_{time.strftime('%Y%m%d_%H%M%S')}.csv"

    with number_of_results_log_path.open("w") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["timestamp", "keyword", "number_of_results", "time_taken"])
   
    return number_of_results_log_path


def log_results(log_path: Path, keyword: str, number_of_results: int, time_taken: float) -> None:
    with log_path.open("a") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([time.strftime('%Y-%m-%d %H:%M:%S'), keyword, number_of_results, time_taken])

def run() -> None:
    engine = setup_engine()
    log_path = setup_logging()
    # Add code to run the engine and perform tests
    number_of_results_list = []
    for keyword in KEYWORDS:
        start_time = time.time()
        results, _ = engine.execute(f"kw('{keyword}')")
        end_time = time.time()

        number_of_results = len(results)
        number_of_results_list.append(number_of_results)
        time_taken = end_time - start_time

        log_results(log_path, keyword, number_of_results, time_taken)
        print(f"Keyword: {keyword}, Results: {number_of_results}, Time taken: {time_taken:.4f} seconds")

    # sort keywords by number of results
    sorted_keywords = sorted(zip(KEYWORDS, number_of_results_list), key=lambda x: x[1], reverse=False)
    print("\nSorted keywords by number of results:")
    kws = []
    for keyword, count in sorted_keywords:
        print(f"{keyword}: {count} results")
        kws.append(keyword)

    print("\nSorted keywords:", kws)

if __name__ == "__main__":
    run()