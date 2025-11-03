#!/bin/bash
set -e
parent_dir=$(dirname "$PWD")

echo "Iterating through lambda functions to install any additional requirements..."
for file in $(ls -d -- $PWD/src/lambdas/*/tests/test*.py); do
    tests_dir=$(dirname $file)
    echo "Switching to:"
    echo $tests_dir
    test_file=$(awk -F'/' '{print $11}' <<<"$file")
    cd $tests_dir
    echo "Running tests..."
    pip3 install -r $tests_dir/../requirements_tests.txt
    echo "Now in directory:"
    echo $(pwd)
    pytest $file
    echo "Running tests complete!"
done