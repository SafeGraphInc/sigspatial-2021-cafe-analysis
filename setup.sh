#!/bin/bash

source ./env.sh 

if [[ ! -e $SPARK_VERSION ]];then
    if [[ ! -e $SPARK_VERSION.tgz ]];then
        wget https://dlcdn.apache.org/spark/spark-3.1.2/$SPARK_VERSION.tgz
    fi
    tar xzf $SPARK_VERSION.tgz
else
    echo "$SPARK_VERSION directory exists, skipping...."
fi


if [[ ! -e apache-sedona-1.1.0-incubating-bin ]];then
    if [[ ! -e apache-sedona-1.1.0-incubating-bin.tar.gz ]];then
        wget https://dlcdn.apache.org/incubator/sedona/1.1.0-incubating/apache-sedona-1.1.0-incubating-bin.tar.gz
    fi
    tar xzf apache-sedona-1.1.0-incubating-bin.tar.gz
else
    echo "apache-sedona-1.1.0-incubating-bin exists, skipping...."
fi

if [[ ! -e geotools-wrapper-1.1.0-25.2.jar ]];then
    wget https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.1.0-25.2/geotools-wrapper-1.1.0-25.2.jar 
fi

cp geotools-wrapper-1.1.0-25.2.jar $SPARK_HOME/jars/
cp -v apache-sedona-1.1.0-incubating-bin/sedona-python-adapter-3.0_2.12-1.1.0-incubating.jar  $SPARK_HOME/jars/

if [[ ! -e data.tbz2 ]];then
    wget https://safegraph-public.s3.us-west-2.amazonaws.com/sigspatial2021/data.tbz2 
fi

tar xjf data.tbz2

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
PySpark
