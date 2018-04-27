#/bin/bash

# set max heap size 64GB
# export HADOOP_HEAPSIZE=65536
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx65536m -Dhadoop.root.logger=INFO,RFA"

#rm -rf ../hadoop-hdfs-project/hadoop-hdfs/target/test/data/
#cd ../hadoop-hdfs-project/hadoop-hdfs
#mvn test -e -Dtest=TestMetaSave#testMetaSaveWithLargeUnderReplicate

hadoop org.apache.hadoop.hdfs.server.namenode.TestMetaSaveScale 

