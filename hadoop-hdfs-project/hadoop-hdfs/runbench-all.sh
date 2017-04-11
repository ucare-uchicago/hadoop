#!/bin/bash

# initialization of Producer and Consumer Threads
p=14
c=1

if [ $# == 6 ]; then
  p=$5
  c=$6
fi

./runbench.sh 10000 4 40 4 $p $c > out-4.txt 2>&1
./runbench.sh 10000 8 80 8 $p $c > out-8.txt 2>&1
./runbench.sh 10000 16 160 16 $p $c > out-16.txt 2>&1
./runbench.sh 10000 32 320 32 $p $c > out-32.txt 2>&1
./runbench.sh 10000 64 640 64 $p $c > out-64.txt 2>&1
./runbench.sh 10000 128 1280 128 $p $c > out-128.txt 2>&1
./runbench.sh 10000 256 2560 256 $p $c > out-256.txt 2>&1
./runbench.sh 10000 512 5120 512 $p $c > out-512.txt 2>&1
