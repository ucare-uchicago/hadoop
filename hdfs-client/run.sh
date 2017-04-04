#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


for i in `seq 1 10`;
do
  hadoop jar $DIR/RandomWriter.jar RandomWriter 1 20 $HOSTNAME-$i > /dev/null 2> /dev/null &
done
