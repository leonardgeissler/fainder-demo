import csv
import json
from math import log
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import hydra
import pandas as pd
from hydra.core.config_store import ConfigStore
from loguru import logger
from omegaconf import DictConfig, OmegaConf

from backend.config import ExecutorType, Metadata, Settings
from backend.engine import Engine, Optimizer
from backend.indices.keyword_op import TantivyIndex
from backend.indices.name_op import HnswIndex as ColumnIndex
from backend.indices.percentile_op import FainderIndex
from torch import chunk

from .config_models import PerformanceConfig
from .eval_performance_test import execute_with_profiling, log_performance_csv, save_profiling_stats
from .generate_eval_test_cases import generate_all_test_cases

# Initialize config store for hydra
cs = ConfigStore.instance()
cs.store(name="performance_config", node=PerformanceConfig)


def initialize_logging(log_dir: Path) -> None:
    """Initialize loguru logger"""
    # Remove default handler and configure custom logging
    logger.remove()
    
    # Add handlers for both file and console
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | {message}",
        level="INFO",
    )
    logger.add(
        log_dir / "logs/query_performance__{time:YYYY-MM-DD HH:mm:ss}.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {message}",
        level="INFO",
        rotation="1 day",
    )


def initialize_engines(config: PerformanceConfig) -> Dict[str, Engine]:
    """Initialize engine instances based on configuration"""
    settings = Settings()  # type: ignore # uses the environment variables
    with settings.metadata_path.open() as file:
        metadata = Metadata(**json.load(file))

    fainder_index = FainderIndex(
        rebinning_path=settings.rebinning_index_path,
        conversion_path=settings.conversion_index_path,
        histogram_path=settings.histogram_path,
        parallel=config.fainder.parallel,
        num_workers=config.fainder.max_workers,
        num_chunks=config.fainder.max_workers,
        chunk_layout=config.fainder.chunk_layout,
    )
    column_index = ColumnIndex(path=settings.hnsw_index_path, metadata=metadata)
    tantivy_index = TantivyIndex(index_path=str(settings.tantivy_path), recreate=False)
    
    engines = {}
    for scenario in config.engines.scenarios:
        # Convert the string executor type from config to the actual enum
        executor = ExecutorType[scenario.executor_type]
        
        engine = Engine(
            tantivy_index=tantivy_index,
            fainder_index=fainder_index,
            hnsw_index=column_index,
            metadata=metadata,
            cache_size=scenario.cache_size,
            executor_type=executor,
        )
        
        # Configure optimizer based on config settings
        engine.optimizer = Optimizer(
            cost_sorting=config.optimizer.cost_sorting,
            keyword_merging=config.optimizer.keyword_merging,
            split_up_junctions=config.optimizer.split_up_junctions
        )
        
        engines[scenario.name] = engine
    
    return engines


def setup_directories(config: PerformanceConfig) -> Dict[str, Any]:
    """Set up directory structure for logs and results"""
    # Get git branch name and hash for organization
    process = subprocess.Popen(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], shell=False, stdout=subprocess.PIPE)
    git_branch = process.communicate()[0].strip().decode('utf-8')
    
    process = subprocess.Popen(['git', 'rev-parse', 'HEAD'], shell=False, stdout=subprocess.PIPE)
    git_head_hash = f"{git_branch}_{process.communicate()[0].strip().decode('utf-8')}"
    
    # Create base directories
    paths = {}
    base_log_dir = Path(config.log_dir)
    base_log_dir.mkdir(exist_ok=True)
    
    log_dir = base_log_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    paths["log_dir"] = log_dir
    
    # Performance logs
    perf_dir = Path(config.performance_log_dir)
    perf_dir.mkdir(exist_ok=True)
    git_heads_dir = perf_dir / "git_heads"
    git_heads_dir.mkdir(exist_ok=True)
    git_hash_dir = git_heads_dir / git_head_hash
    git_hash_dir.mkdir(exist_ok=True)
    all_results_dir = git_hash_dir / "all"
    all_results_dir.mkdir(exist_ok=True)
    paths["perf_dir"] = perf_dir
    paths["git_hash_dir"] = git_hash_dir
    paths["all_results_dir"] = all_results_dir

    # Profiling logs
    profiling_dir = git_hash_dir / Path(config.profiling_log_dir)
    profiling_dir.mkdir(exist_ok=True)
    profiling_raw_dir = profiling_dir / "raw"
    profiling_raw_dir.mkdir(exist_ok=True)
    paths["profiling_dir"] = profiling_dir
    paths["profiling_raw_dir"] = profiling_raw_dir
    
    # Individual test directories
    individual_log_dirs = {}
    for test_name in config.experiment.enabled_tests:
        test_log_dir = git_hash_dir / test_name
        test_log_dir.mkdir(exist_ok=True)
        individual_log_dirs[test_name] = test_log_dir
    paths["individual_log_dirs"] = individual_log_dirs
    
    return paths


def create_csv_files(paths: Dict[str, Any]) -> Dict[str, Any]:
    """Create CSV files for results"""
    csv_paths = {}
    timestamp_str = time.strftime("%Y%m%d_%H%M%S")
    
    # Main performance CSV
    perf_csv_path = paths["all_results_dir"] / f"performance_metrics_{timestamp_str}.csv"
    with perf_csv_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "timestamp", "category", "test_name", "query", "scenario",
            "execution_time", "results_consistent", "fainder_mode",
            "num_results", "ids", "num_terms", "id_str", "write_groups_used", "write_groups_actually_used", "fainder_parallel",
            "fainder_max_workers", "fainder_contiguous_chunks", "optimizer_cost_sorting", "optimizer_keyword_merging", "optimizer_split_up_junctions"
        ])
    csv_paths["main_perf_csv"] = perf_csv_path
    
    # Profiling CSV
    profile_csv_path = paths["profiling_dir"] / f"profiling_metrics_{timestamp_str}.csv"
    with profile_csv_path.open("w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([
            "timestamp", "category", "test_name", "query", "scenario",
            "fainder_mode", "ncalls", "tottime", "percall_tottime",
            "cumtime", "percall_cumtime", "function_info"
        ])
    csv_paths["profile_csv"] = profile_csv_path
    
    # Individual test CSVs
    individual_csv_paths = {}
    for test_name, test_dir in paths["individual_log_dirs"].items():
        test_csv_path = test_dir / f"performance_metrics_{test_name}_{timestamp_str}.csv"
        with test_csv_path.open("w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "timestamp", "category", "test_name", "query", "scenario",
                "execution_time", "results_consistent", "fainder_mode",
                "num_results", "ids", "num_terms", "id_str", "write_groups_used", "write_groups_actually_used", "fainder_parallel",
                "fainder_max_workers", "fainder_contiguous_chunks", "optimizer_cost_sorting", "optimizer_keyword_merging", "optimizer_split_up_junctions"
            ])
        individual_csv_paths[test_name] = test_csv_path
    
    csv_paths["individual_csvs"] = individual_csv_paths
    return csv_paths


def run_test_case(
    engines: Dict[str, Engine],
    category: str,
    test_name: str,
    test_case: Dict[str, Any],
    csv_paths: Dict[str, Any],
    disable_profiling: bool,
    fainder_modes: List[str],
    config: PerformanceConfig,
    num_runs: int = 1
) -> None:
    """Run a single test case across all engines and modes"""
    query = test_case["query"]
    ids = test_case.get("ids", [])
    num_terms = test_case.get("num_terms", 0)
    keyword_id = test_case.get("keyword_id")
    percentile_id = test_case.get("percentile_id")
    id_str = ""
    
    if keyword_id:
        id_str = keyword_id
    elif percentile_id:
        id_str = percentile_id
        
    for mode in fainder_modes:
        for _ in range(num_runs):
            # Run query with each engine scenario
            timings = {}
            results = {}
            write_groups_actually_used: dict[int, int] = {}
            write_groups_used: dict[int, int] = {}

            for scenario, engine in engines.items():
                result, execution_time, stats_io = execute_with_profiling(
                    engine, query, {}, mode, disable_profiling=disable_profiling
                )
                timings[scenario] = execution_time
                results[scenario] = result
                
                # Save profiling data if enabled
                if not disable_profiling:
                    save_profiling_stats(
                        stats_io, csv_paths["profile_csv"], category, test_name, 
                        query, scenario, mode
                    )

                # Get write groups data 
                if hasattr(engine.executor, "write_groups_actually_used") and hasattr(engine.executor, "write_groups_used"):
                    write_groups_actually_used: dict[int, int] = getattr(engine.executor, "write_groups_actually_used", {})
                    write_groups_used: dict[int, int] = getattr(engine.executor, "write_groups_used", {})

            # Check result consistency
            first_result = next(iter(results.values()))
            is_consistent = all(set(result) == set(first_result) for result in results.values())
            
            # Log to main CSV
            log_performance_csv(
                csv_paths["main_perf_csv"],
                category,
                test_name,
                query,
                timings,
                results,
                is_consistent,
                mode,
                ids,
                num_terms,
                id_str,
                write_groups_used,
                write_groups_actually_used,
                config.fainder.parallel,
                config.fainder.max_workers,
                config.fainder.chunk_layout,
                config.optimizer.cost_sorting,
                config.optimizer.keyword_merging,
            )
            
            # Log to individual test CSV
            if category in csv_paths["individual_csvs"]:
                log_performance_csv(
                    csv_paths["individual_csvs"][category],
                    category,
                    test_name,
                    query,
                    timings,
                    results,
                    is_consistent,
                    mode,
                    ids,
                    num_terms,
                    id_str,
                    write_groups_used,
                    write_groups_actually_used,
                    config.fainder.parallel,
                    config.fainder.max_workers,
                    config.fainder.chunk_layout,
                    config.optimizer.cost_sorting,
                    config.optimizer.keyword_merging,
                )
            
            # Log to console
            performance_log = {
                "category": category,
                "test_name": test_name,
                "query": query,
                "metrics": {
                    "timings": timings,
                },
                "results_consistent": is_consistent,
            }
            logger.info(f"PERFORMANCE_DATA: {performance_log}")
            
            if not is_consistent:
                logger.warning(f"Results inconsistent for {category} - {test_name} with query: {query}")

@hydra.main(version_base=None, config_path="conf", config_name="config")
def main(hydra_config: DictConfig) -> None:
    """Main entry point for performance testing"""
    logger.info("Starting performance tests")
    logger.info(f"Hydra config: {OmegaConf.to_yaml(hydra_config)}")
    # Convert hydra config to pydantic model
    config = PerformanceConfig.from_dict(OmegaConf.to_container(hydra_config, resolve=True)) # type: ignore
    logger.info(f"Performance config: {config}")
    
    # Setup directories and files
    paths = setup_directories(config)
    csv_paths = create_csv_files(paths)

    # Load test cases
    if config.use_json_test_cases:
        logger.info("Using JSON test cases")
        with Path("test_cases/performance_test_cases.json").open("r") as f:
            test_cases = json.load(f)
    else:
        logger.info("Using generated test cases")
        test_cases = generate_all_test_cases(config)
    logger.info(f"Generated {len(test_cases)} test cases")  
    
    # Initialize logging
    initialize_logging(paths["log_dir"])
    
    # Initialize engines
    engines = initialize_engines(config)
    logger.info(f"Initialized engines: {list(engines.keys())}")
    logger.info(f"Using modes: {config.experiment.fainder_modes}")
    
    # Run all tests
    for category, data in test_cases.items():
        logger.info(f"Starting category: {category}")
        for test_name, test_case in data["queries"].items():
            run_test_case(
                engines=engines,
                category=category,
                test_name=test_name,
                test_case=test_case,
                csv_paths=csv_paths,
                disable_profiling=config.disable_profiling,
                fainder_modes=config.experiment.fainder_modes,
                config=config,
                num_runs=config.experiment.num_runs  # Pass the number of runs
            )
            
    # Summarize results
    try:
        results_df = pd.read_csv(csv_paths["main_perf_csv"])
        summary = results_df.groupby(['category', 'scenario', 'fainder_mode'])['execution_time'].mean()
        logger.info(f"Test execution summary:\n{summary}")
    except Exception as e:
        logger.error(f"Failed to generate summary stats: {e}")
    
    logger.info(f"All tests completed. Results saved to {paths['git_hash_dir']}")


if __name__ == "__main__":
    main()

