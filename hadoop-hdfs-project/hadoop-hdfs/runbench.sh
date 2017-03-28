#!/bin/bash

clsetup
hdfs namenode -format -nonInteractive
hadoop jar $HADOOP_PREFIX/share/hadoop/hdfs/hadoop-hdfs-2.7.1-tests.jar org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op blockReport -reports 1 -blocksPerReport $1 -blocksPerFile $3 -writerPoolSize $4 -datanodes $2
