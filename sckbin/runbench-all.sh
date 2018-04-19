#!/bin/bash

./runbench.sh 10000 4 20 4 > out-4.txt 2>&1
./runbench.sh 10000 8 40 8 > out-8.txt 2>&1
./runbench.sh 10000 16 80 16 > out-16.txt 2>&1
./runbench.sh 10000 32 160 32 > out-32.txt 2>&1
./runbench.sh 10000 64 320 64 > out-64.txt 2>&1
./runbench.sh 10000 128 640 128 > out-128.txt 2>&1
./runbench.sh 10000 256 1280 256 > out-256.txt 2>&1
./runbench.sh 10000 512 2560 512 > out-512.txt 2>&1
