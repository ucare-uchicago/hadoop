#!/bin/bash

# set max heap size 16GB
export HADOOP_HEAPSIZE=16384
hadoop org.apache.hadoop.hdfs.server.namenode.snapshot.TestSnapshotDiffReportScale > output.txt 2>&1 

