#!/bin/bash

. cluster_topology.sh

cp $PSBIN/ucare_se_conf/dmck-hack/* $HADOOP_CONF_DIR/
sed_replaceconf.sh
