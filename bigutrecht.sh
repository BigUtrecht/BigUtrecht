#!/usr/bin/env bash

# Set correct environment
export SPARK_HOME=/mnt/spark-2.0.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64

/usr/bin/python2.7 ~/BigUtrecht/BigUtrecht/app.py