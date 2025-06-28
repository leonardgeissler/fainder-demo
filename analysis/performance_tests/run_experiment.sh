#!/bin/bash

# Run a performance experiment with hydra config

# Default config
CONFIG="default"

# Parse command line args
while getopts "c:" opt; do
  case ${opt} in
    c )
      CONFIG=$OPTARG
      ;;
    \? )
      echo "Invalid option: -$OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Option -$OPTARG requires an argument." 1>&2
      exit 1
      ;;
  esac
done

echo "Running experiment with config: $CONFIG"
python -m analysis.performance_tests.run experiment=$CONFIG
