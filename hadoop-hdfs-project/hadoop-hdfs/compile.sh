#!/bin/bash

mvn package install -Pdist -DskipTests
cp target/hadoop-hdfs-2.7.1.jar $HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-2.7.1.jar
cp target/hadoop-hdfs-2.7.1-tests.jar $HADOOP_HOME/share/hadoop/hdfs/hadoop-hdfs-2.7.1-tests.jar
