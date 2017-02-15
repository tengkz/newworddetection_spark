#!/bin/bash

/home/hadoop/spark/bin/spark-submit \
    --class Test \
    --conf spark.driver.extraClassPath=/home/hadoop/hiveconf/metastore/h30_us \
    --master yarn-client \
    /home/hztengkezhen/learn/spark/Test.jar
