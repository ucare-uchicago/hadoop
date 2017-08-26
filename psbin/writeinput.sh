#!/bin/bash

. cluster_topology.sh

clstop
cp $PSBIN/ucare_se_conf/writeconf/* $HADOOP_CONF_DIR/
sed_replaceconf.sh
clcleanlogs
clreset
clsetup
ssh -t $HDFS_NN "dfsformat;"

clstart
sleep 10
ssh -t $CLIENT_NODE "$PSBIN/prep_run.sh > /tmp/writeinput.log &" 
