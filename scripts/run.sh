#!/bin/bash

set -euxo pipefail
cd "$(git rev-parse --show-toplevel)"
FAINDER_DEMO_HOME=$(pwd)
export FAINDER_DEMO_HOME

# Lucene service
java --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector \
    -jar lucene/target/fainder-demo-1.0-SNAPSHOT.jar

# Fainder service
(
    cd backend || exit
    fastapi dev server.py
)

# UI
(
    cd ui || exit
    npm run dev
)
