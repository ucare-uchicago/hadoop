#!/bin/bash

./runbench.sh 10000 4 40 4 > out-4.txt 2>&1
./runbench.sh 10000 8 80 8 > out-8.txt 2>&1
./runbench.sh 10000 16 160 16 > out-16.txt 2>&1
./runbench.sh 10000 32 320 32 > out-32.txt 2>&1
./runbench.sh 10000 64 640 64 > out-64.txt 2>&1
./runbench.sh 10000 128 1280 128 > out-128.txt 2>&1
./runbench.sh 10000 256 2560 256 > out-256.txt 2>&1
./runbench.sh 10000 512 5120 512 > out-512.txt 2>&1
