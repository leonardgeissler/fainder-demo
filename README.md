<p align="center">
  <picture>
    <img alt="Fainder logo" src="https://github.com/user-attachments/assets/41686649-f1c1-4b60-824e-80c322c5da85" width="300">
  </picture>
</p>

# Fainder Demo

![Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Flbhm%2Ffainder-demo%2Fmain%2Fpyproject.toml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![GitHub License](https://img.shields.io/github/license/lbhm/fainder-demo)

This repository contains the source code for our demonstration of Fainder, a fast and accurate
index for distibution-aware dataset search. The demo consists of three components:

- **Frontend**: Web-based user interface for interacting with the search engine.
- **Fainder**: Backend service responsible for query parsing and processing [percentile predicates](https://doi.org/10.14778/3681954.3681999).
- **Lucene**: An extension of [Apache Lucene](https://lucene.apache.org/) for handling keyword predicates.

The repository is structured as follows:

```bash
fainder/
├── data  # placeholder for dataset profiles and index files
├── fainder_demo  # main backend component (Python)
├── lucene  # Lucene extension (Java)
├── scripts  # scripts for installing and starting components (Bash)
└── ui  # user interface (JavaScript)
```

## Getting Started

JAVA Installation:

Download: (extract to lib folder)
https://dlcdn.apache.org/lucene/java/10.0.0/lucene-10.0.0.tgz
https://search.maven.org/artifact/com.google.code.gson/gson/2.11.0/jar?eh=

Add to lib folder and add to classpath in your IDE

TODO

### Developer Setup

```bash
# Follow the steps above until you have activated your virtual environment
pip install -e ".[dev]"
pre-commit install
```
