#!/bin/bash

rsync -av --delete empty_dir/ target/
rm -rf target
rm /tmp/hadoop-ucare/logs/hadoop/*.log
