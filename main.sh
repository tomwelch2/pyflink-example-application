#!/bin/bash

python_script="/home/tom/Documents/pythonfiles/flink_stuff/git_project/scripts/script.py"

cd $FLINK_HOME && bin/start-cluster.sh

bin/flink run --python "$python_script"
