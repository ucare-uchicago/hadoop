#!/bin/bash

cd ../hadoop-hdfs-project/hadoop-hdfs
mvn package -Pdist -DskipTests
cp target/hadoop-hdfs-2.7.1-test* ../../hadoop-dist/target/hadoop-2.7.1/share/hadoop/hdfs/

