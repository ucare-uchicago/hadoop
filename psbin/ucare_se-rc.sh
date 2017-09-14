#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export PR=${DIR}/../..
export SWIMDIR=${PR}/SWIM
export TESTDIR=${SWIMDIR}/workloadSuite/generatedWorkloads/st-FB2010_Proper_30node

export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
export HADOOP_PREFIX=/home/ubuntu/hadoop/hadoop-dist/target/hadoop-2.1.0-beta
export HADOOP_CONF_DIR=${DIR}/ucare_se_conf/hadoop-etc/hadoop-2.1.0-beta
export HADOOP_HOME=${HADOOP_PREFIX}
export HADOOP_LOG_DIR=/tmp/hadoop-ucare/logs/hadoop
export YARN_LOG_DIR=/tmp/hadoop-ucare/logs/yarn
export HADOOP_MAPRED_LOG_DIR=/tmp/hadoop-ucare/logs/mapred

export PSBIN=${DIR}
export PATH=${HADOOP_PREFIX}/bin:${HADOOP_PREFIX}/sbin:${PSBIN}:${PATH}

alias h="history 25"
alias ls="ls --color"
alias la="ls -a"
alias lf="ls -FA"
alias ll="ls -lA"

alias pr="cd $PR"
alias e="vim"
alias hp="cd $HADOOP_PREFIX"
alias hconf="cd $HADOOP_CONF_DIR"
alias hlogs="cd /tmp/logs/yarn/userlogs/"
alias m="make"
alias mall="make all"
alias n0="ssh node-0"
alias ben="cd $TESTDIR/workGenLogs/"
alias mjl="mapred job -list"
alias mjs="mapred job -status"
alias psbin="cd $PSBIN"
alias samc="cd ~/samc"
alias exp="cd /tmp/dmck-hadoop"
