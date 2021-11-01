#!/bin/bash 
SPARK_VERSION=spark-3.1.2-bin-hadoop2.7
export SPARK_HOME=$PWD/$SPARK_VERSION
export PATH=$PATH:$SPARK_HOME/bin
export PYTHON_PATH=$SPARK_HOME/python
export SEDONA_BINS=$PWD/apache-sedona-1.1.0-incubating-bin
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889" 
export SPARK_LOCAL_IP="127.0.0.1"