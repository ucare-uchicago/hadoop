#!/bin/bash

rsync -av --delete empty_dir/ build/
rm -rf build
rm /tmp/hadoop-ucare/logs/hadoop/*.log
