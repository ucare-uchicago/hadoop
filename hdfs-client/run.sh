#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

hadoop jar $DIR/RandomWriter.jar RandomWriter 10 20 $HOSTNAME > /dev/null 2> /dev/null
