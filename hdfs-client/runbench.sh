#!/bin/bash

source $PSBIN/cluster_topology.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COMMAND="nohup $DIR/run.sh"

clstop
clreset
clsetup
dfsformat
clstart

sleep 10

hdfs dfs -mkdir -p /home/ubuntu

echo "Running command $COMMAND"

for i in `seq 1 $MAX_NODE`;
do
  echo "Running command on $HOSTNAME_PREFIX$i"
  ssh $HOSTNAME_PREFIX$i "$COMMAND" &
done

