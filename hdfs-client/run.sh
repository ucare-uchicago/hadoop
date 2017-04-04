#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


for i in `seq 1 $1`;
do
  hadoop jar $DIR/RandomWriter.jar RandomWriter 1 160 $HOSTNAME-$i > /dev/null 2> /dev/null &
done
