#!/bin/bash
# *** INTERNAL DOCUMENT --- NOT FOR DISTRIBUTION ***
function run_test() {
        if [ -e "tests/action_tests/$1/test_action.py" ]; then
                if [ -z $2 ]; then
                        echo Running test $1
                        python -m unittest tests.action_tests.$1.test_action > test_$1.out
                else
                        echo Running test $1 - $specific_test
                        python -m unittest tests.action_tests.$1.test_action.TestAction.$specific_test > test_$1.out
                fi
        else
                echo "ERROR: Test $1 not found"
        fi
}

if [ ! -z "$1" ]; then
        specific_test=""
        if [ ! -z "$2" ]; then
                specific_test=$2
        fi
        run_test $1 $specific_test
else
        ls tests/action_tests | while read file; do
                if [[ $file == "__"* ]]; then
                        continue
                fi
                if [ -d "tests/action_tests/${file}" ]; then
                        run_test $file
                fi
        done
fi
