#!/bin/bash

declare -a BENCHMARKS=("hashes.py" "routing.py" "network.py")
VENV="../venv/bin/activate"

# args
TAG="27072023"
OUTPUT="./test"
ITERATIONS=10
NETWORK_SIZE=10000
K=20

# activate the venv
source $VENV
if [ $? -eq 0 ]; then
  echo "venv successfully sourced"
else
  echo "unable to source venv at $VENV , does it exist?"
  exit 1
fi

for benchmark in "${BENCHMARKS[@]}"
do
  echo "launching benchmark $benchmark"
  python $benchmark -t $TAG -o $OUTPUT -i $ITERATIONS -n $NETWORK_SIZE -k $K
  if [ $? -eq 0 ]; then
    echo "benchmark done"
    echo ""
  else
    echo "error running benchmark"
    exit 1
  fi
done