#!/bin/bash

declare -a TESTS=("tests/test_hashes.py" "tests/test_routing.py" "tests/test_network.py")
VENV="prod-env/bin/activate"

# activate the venv
source $VENV
if [ $? -eq 0 ]; then
  echo "venv successfully sourced"
else
  echo "unable to source venv at $VENV , does it exist?"
  exit 1
fi

for t in "${TESTS[@]}"
do
  echo "launching benchmark $t"
  python -m unittest $t
done