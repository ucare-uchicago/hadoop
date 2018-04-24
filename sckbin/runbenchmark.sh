#!/bin/bash

rm -rf build

# set max heap size 64GB
export HADOOP_HEAPSIZE=65536
hadoop org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotDiffReportScale > output.txt 2>&1 

