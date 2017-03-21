#!/bin/bash

hadoop jar $HADOOP_PREFIX/share/hadoop/hdfs/hadoop-hdfs-2.7.1-tests.jar org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op blockReport -reports 1 -blocksPerReport 10000 -blocksPerFile 106 -writerPoolSize 1 -datanodes 32
