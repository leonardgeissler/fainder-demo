# Sample terms for keyword queries
TERMS = [
    "data",
    "science",
    "machine",
    "learning",
    "artificial",
    "intelligence",
    "neural",
    "network",
    "deep",
    "model",
    "algorithm",
    "analytics",
    "statistics",
    "regression",
    "classification",
    "prediction",
    "mining",
    "visualization",
    "feature",
    "extraction",
    "clustering",
    "ensemble",
    "optimization",
    "bias",
    "variance",
    "overfitting",
    "inference",
    "validation",
    "precision",
    "recall",
    "analysis",
    "research",
    "system",
    "method",
    "process",
    "technology",
    "framework",
    "solution",
    "approach",
    "implementation",
    "development",
    "application",
    "function",
    "structure",
    "performance",
    "quality",
    "result",
    "evaluation",
    "experiment",
    "theory",
    "concept",
]

LENGTH_TERMS = 10


def generate_keyword_test_cases() -> list[tuple[str, str, str]]:
    """Generate test cases for comparing merged and unmerged keyword queries.

    Returns:
        List of tuples (test_name, merged_query, unmerged_query)
    """
    test_cases = []

    for n in range(1, LENGTH_TERMS + 1):
        i = len(TERMS) // n
        for j in range(i):
            terms = TERMS[j * n : (j + 1) * n]
            test_name = f"{n}_terms_{j}"
            merged = f"kw('{' AND '.join(terms)}')"
            unmerged = " AND ".join([f"kw('{term}')" for term in terms])
            test_cases.append((test_name, merged, unmerged))

    return test_cases


def print_test_cases() -> None:
    """Print test cases for debugging purposes."""
    for name, merged, unmerged in generate_keyword_test_cases():
        print(f"Test: {name}")
        print(f"  Merged:   {merged}")
        print(f"  Unmerged: {unmerged}")
        print()


if __name__ == "__main__":
    print_test_cases()
