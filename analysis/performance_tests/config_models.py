from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List

@dataclass
class KeywordConfig:
    """Configuration for keyword-related settings"""
    default_keywords: List[str] = field(
        default_factory=lambda: ["test", "born", "by"],
        metadata={"description": "Default keywords to use in test queries"}
    )
    default_col_names: List[str] = field(
        default_factory=lambda: ["age", "date"],
        metadata={"description": "Default column names to use in test queries"}
    )
    default_ks: List[int] = field(
        default_factory=lambda: [0, 2, 5],
        metadata={"description": "Default k values for column proximity"}
    )
    logical_operators: List[str] = field(
        default_factory=lambda: ["AND"],
        metadata={"description": "Logical operators to use in query combinations"}
    )


@dataclass
class PercentileConfig:
    """Configuration for percentile-related settings"""
    default_thresholds: List[int] = field(
        default_factory=lambda: [100, 1000000],
        metadata={"description": "Threshold values for percentile operations"}
    )
    default_percentiles: List[float] = field(
        default_factory=lambda: [0.5, 0.9],
        metadata={"description": "Percentile values to use in test queries"}
    )
    default_operators: List[str] = field(
        default_factory=lambda: ["ge", "le"],
        metadata={"description": "Operators for percentile comparisons"}
    )


@dataclass
class QueryGenerationConfig:
    """Configuration for query generation parameters"""
    min_num_terms_query: int = field(
        default=2,
        metadata={"description": "Minimum number of terms in generated queries"}
    )
    max_num_terms_query: int = field(
        default=4,
        metadata={"description": "Maximum number of terms in generated queries"}
    )
    max_num_query_per_num_terms: int = field(
        default=20,
        metadata={"description": "Maximum number of queries to generate per term count"}
    )
    max_num_mixed_terms_with_fixed_structure: int = field(
        default=30,
        metadata={"description": "Maximum number of mixed terms with fixed structure"}
    )
    max_num_mixed_terms_extended_with_fixed_structure: int = field(
        default=20,
        metadata={"description": "Maximum number of mixed terms with extended fixed structure"}
    )
    max_num_of_nested_terms_per_level: int = field(
        default=10,
        metadata={"description": "Maximum number of nested terms per level"}
    )
    min_nested_level: int = field(
        default=2,
        metadata={"description": "Minimum nesting level for queries"}
    )
    max_nested_level: int = field(
        default=4,
        metadata={"description": "Maximum nesting level for queries"}
    )


@dataclass
class EngineConfig:
    """Configuration for a single engine"""
    name: str = field(metadata={"description": "Name of the engine configuration"})
    executor_type: str = field(metadata={"description": "Type of executor to use"})
    cache_size: int = field(default=0, metadata={"description": "Cache size for the engine"})


@dataclass
class EnginesConfig:
    """Configuration for all engines"""
    scenarios: List[EngineConfig] = field(
        default_factory=lambda: [
            EngineConfig(name="simple", executor_type="SIMPLE"),
            EngineConfig(name="perfiltering", executor_type="PREFILTERING"),
            EngineConfig(name="parrallel_engine", executor_type="THREADED"),
            EngineConfig(name="parallel_prefiltering_engine", executor_type="THREADED_PREFILTERING"),
        ],
        metadata={"description": "Engine scenarios to test"}
    )


@dataclass
class ExperimentConfig:
    """Configuration for experiment parameters"""
    enabled_tests: List[str] = field(
        default_factory=lambda: [
            "base_keyword_queries",
            "base_percentile_queries",
            "percentile_combinations",
            "mixed_combinations_with_fixed_structure",
            "mixed_combinations_with_fixed_structure_extented",
            "early_exit",
            "multiple_percentile_combinations",
            "multiple_percentile_combinations_with_kw"
        ],
        metadata={"description": "Test categories to enable"}
    )
    fainder_modes: List[str] = field(
        default_factory=lambda: ["low_memory", "full_precision", "full_recall", "exact"],
        metadata={"description": "Fainder modes to test"}
    )


@dataclass
class PerformanceConfig:
    """Main configuration for performance tests"""
    # Paths for logs
    log_dir: str = field(default="logs", metadata={"description": "Base directory for logs"})
    performance_log_dir: str = field(default="logs/performance", metadata={"description": "Directory for performance logs"})
    profiling_log_dir: str = field(default="logs/profiling", metadata={"description": "Directory for profiling logs"})
    
    # Execution settings
    disable_profiling: bool = field(default=True, metadata={"description": "Whether to disable profiling"})
    use_json_test_cases: bool = field(default=False, metadata={"description": "Whether to use JSON test cases"})
    
    # Test configuration
    keywords: KeywordConfig = field(default_factory=KeywordConfig, metadata={"description": "Keyword configuration"})
    percentiles: PercentileConfig = field(default_factory=PercentileConfig, metadata={"description": "Percentile configuration"})
    query_generation: QueryGenerationConfig = field(default_factory=QueryGenerationConfig, metadata={"description": "Query generation configuration"})
    engines: EnginesConfig = field(default_factory=EnginesConfig, metadata={"description": "Engines configuration"})
    experiment: ExperimentConfig = field(default_factory=ExperimentConfig, metadata={"description": "Experiment configuration"})
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> "PerformanceConfig":
        """Create a PerformanceConfig from a dictionary (e.g., from hydra)"""
        # Convert hydra's structured config to dict and handle nested configs
        if hasattr(config_dict, "_target_"):
            # This is a hydra structured config
            return cls(**config_dict)
        
        keywords_dict = config_dict.get("keywords", {})
        percentiles_dict = config_dict.get("percentiles", {})
        query_generation_dict = config_dict.get("query_generation", {})
        engines_dict = config_dict.get("engines", {})
        experiment_dict = config_dict.get("experiment", {})
        
        # Create nested configs
        keywords = KeywordConfig(**keywords_dict) if keywords_dict else KeywordConfig()
        percentiles = PercentileConfig(**percentiles_dict) if percentiles_dict else PercentileConfig()
        query_generation = QueryGenerationConfig(**query_generation_dict) if query_generation_dict else QueryGenerationConfig()
        
        # Handle the engines configuration which is more complex
        engines = EnginesConfig()
        if engines_dict and "scenarios" in engines_dict:
            engines.scenarios = [
                EngineConfig(**scenario) if isinstance(scenario, dict) else scenario
                for scenario in engines_dict["scenarios"]
            ]
        
        experiment = ExperimentConfig(**experiment_dict) if experiment_dict else ExperimentConfig()
        
        # Create the main config
        return cls(
            log_dir=config_dict.get("log_dir", "logs"),
            performance_log_dir=config_dict.get("performance_log_dir", "logs/performance"),
            profiling_log_dir=config_dict.get("profiling_log_dir", "logs/profiling"),
            disable_profiling=config_dict.get("disable_profiling", True),
            use_json_test_cases=config_dict.get("use_json_test_cases", False),
            keywords=keywords,
            percentiles=percentiles,
            query_generation=query_generation,
            engines=engines,
            experiment=experiment
        )
