#!/usr/bin/env bash

# Set correct environment
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SPARK_HOME=/mnt/spark-2.0.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib:/usr/local/lib64

/usr/bin/python2.7 $DIR/app.py