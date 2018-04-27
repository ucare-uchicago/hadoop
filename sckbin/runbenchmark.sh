#/bin/bash

rm -rf ../hadoop-hdfs-project/hadoop-hdfs/target/test/data/

# set max heap size 64GB
export HADOOP_HEAPSIZE=65536

cd ../hadoop-hdfs-project/hadoop-hdfs
mvn test -e -Dtest=TestMetaSave#testMetaSaveWithLargeUnderReplicate

#sleep 5
#echo "methodTraceDump=true" > $SFIELD_INPUT

