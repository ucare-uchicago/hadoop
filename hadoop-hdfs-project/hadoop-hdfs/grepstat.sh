#!/bin/bash

grep "creat min" $1 | awk '{print $8}'
grep "creat max" $1 | awk '{print $8}'
grep "creat avg" $1 | awk '{print $8}'
grep "ibrSt min" $1 | awk '{print $8}'
grep "ibrSt max" $1 | awk '{print $8}'
grep "ibrSt avg" $1 | awk '{print $8}'
grep "lifetime (ms)" $1 | awk '{print $8}'
grep "%life in IBR" $1 | awk '{print $9}'
grep "IBR/ms" $1 | awk '{print $7}'
grep "%life in create" $1 | awk '{print $9}'
grep "%life in lock" $1 | awk '{print $9}'
