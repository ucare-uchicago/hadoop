#!/bin/bash

hdfs dfs -rm -r /users/riza/output

hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.1.2-SNAPSHOT.jar wordcount /users/riza/input /users/riza/output

