#!/bin/bash

cd ..
ant jar
ant jar-test
rm -r conf
cp -r ../default-conf conf

