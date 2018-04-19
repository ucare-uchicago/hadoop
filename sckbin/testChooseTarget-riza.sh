#!/bin/sh

cd ..

rm -rf build/test/data/

# compile the code
# ant jar
ant jar

#cd build/hadoop-0.12.4-dev/
cd build/

alljars=""
for jar in `ls ../lib/*.jar`; do
  alljars="$alljars:$jar"
done
for jar in `ls ../lib/jetty-ext/*.jar`; do
  alljars="$alljars:$jar"
done
for jar in `ls ./*.jar`; do
  alljars="$alljars:$jar"
done
#java -cp $alljars org.apache.hadoop.conf.Configuration
echo $alljars
java -cp $alljars -Xmx64G -Dlog4j.configuration="src/test/log4j.properties" edu.uchicago.cs.ucare.scale.MultiThreadChooseTargetTest 5 1
