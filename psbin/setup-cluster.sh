#!/bin/bash

. cluster_topology.sh

clstop
clcleanlogs
clreset
clsetup
ssh -t $HDFS_NN "dfsformat;"

clstart
