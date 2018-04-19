#!/bin/bash

# normal execution on hadoop-2.7.1
# DO NOT uncomment this! this is just for reference
# java -cp "/proj/ucare/git/hadoop-ucare/psbin/ucare_se_conf/hadoop-etc/hadoop-2.7.1:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/common/lib/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/common/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/hdfs:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/hdfs/lib/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/hdfs/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/yarn/lib/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/yarn/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/mapreduce/lib/*:/proj/ucare/riza/hadoop-2.7.1.faread/share/hadoop/mapreduce/*:/usr/lib/jvm/java-7-openjdk-amd64lib/tools.jar:/proj/ucare/riza/hadoop-2.7.1.faread/contrib/capacity-scheduler/*.jar" org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark

cd ..

# compile, package, and install this hadoop
#mvn package install -Pdist -DskipTests

# execute benchmark on hadoop-0.22
ROOT=`pwd`
HPATH="$ROOT/hadoop-project-dist/target/hadoop-0.24.0-SNAPSHOT"
CLASSPATH="$ROOT/sckbin/hadoop-etc:$HPATH/share/hadoop/common/lib/*:$HPATH/share/hadoop/common/*:$HPATH/share/hadoop/hdfs:$HPATH/share/hadoop/hdfs/lib/*:$HPATH/share/hadoop/hdfs/*:$HPATH/share/hadoop/yarn/lib/*:$HPATH/share/hadoop/yarn/*:$HPATH/share/hadoop/mapreduce/lib/*:$HPATH/share/hadoop/mapreduce/*:/usr/lib/jvm/java-7-openjdk-amd64lib/tools.jar"

cd hadoop-project-dist/target/
bash dist-layout-stitching.sh
cd ../../

java -cp $CLASSPATH -Dproc_namenode -Xmx1000m -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/tmp/hadoop-ucare/logs/hadoop -Dhadoop.log.file=hadoop.log -Dhadoop.id.str=$USER -Dhadoop.root.logger=INFO,console org.apache.hadoop.hdfs.server.namenode.NameNode -format
 
java -cp $CLASSPATH -Djava.net.preferIPv4Stack=true org.apache.hadoop.hdfs.server.namenode.NNThroughputBenchmark -op blockReport -datanodes 5 -reports 1 -blocksPerReport 5000 -blocksPerFile 100
