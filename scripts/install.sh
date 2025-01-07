#!/bin/bash

# UI setup
(
    cd ui || exit
    npm install
)

# Backend setup
(
    cd backend || exit
    virtualenv venv
    # shellcheck source=backend/venv/bin/activate
    source venv/bin/activate
    pip install -e ".[dev]"
)

# Lucene setup
(
    cd lucene || exit
    mvn clean package
)
