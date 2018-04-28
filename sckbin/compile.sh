#!/bin/bash

cd ../hadoop-hdfs-project/hadoop-hdfs
mvn package -Pdist -DskipTests
cp target/hadoop-hdfs-2.9.0-test* ../../hadoop-dist/target/hadoop-2.9.0/share/hadoop/hdfs/

