#!/bin/bash

python_script=*ENTER LOCATION OF PYTHON SCRIPT HERE*

cd $FLINK_HOME && bin/start-cluster.sh

bin/flink run --python "$python_script"
