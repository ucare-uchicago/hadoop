#!/bin/bash

source $PSBIN/cluster_topology.sh

TASKDIV=`expr 1000 / $MAX_NODE`
TASKDIV1=`expr 1000 / $MAX_NODE + 1`
TASKMOD=`expr 1000 % $MAX_NODE`

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COMMAND="nohup $DIR/run.sh"

clstop
clreset
clsetup
dfsformat
clstart

sleep 10

hdfs dfs -mkdir -p /home/ubuntu

for i in `seq 1 $MAX_NODE`;
do
  NODECOMM=$COMMAND
  if [ "$i" -le "$TASKMOD" ]
  then
    echo "Running $COMMAND $TASKDIV1 on $HOSTNAME_PREFIX$i"
    ssh $HOSTNAME_PREFIX$i "$COMMAND $TASKDIV1" &
  else
    echo "Running $COMMAND $TASKDIV on $HOSTNAME_PREFIX$i"
    ssh $HOSTNAME_PREFIX$i "$COMMAND $TASKDIV" &
  fi
done

sleep 300
hdfs dfs -touchz /home/ubuntu/dumpstats.txt
echo "Benchmark done!"
