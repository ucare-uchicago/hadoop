#!/bin/bash

mvn package install -Pdist -DskipTests
cp target/hadoop-hdfs-2.7.1.jar /home/cc/hadoop-2.7.1/hadoop-dist/target/hadoop-2.7.1/share/hadoop/hdfs/hadoop-hdfs-2.7.1.jar
cp target/hadoop-hdfs-2.7.1-tests.jar /home/cc/hadoop-2.7.1/hadoop-dist/target/hadoop-2.7.1/share/hadoop/hdfs/hadoop-hdfs-2.7.1-tests.jar
