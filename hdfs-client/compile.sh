#!/bin/bash

hadoop com.sun.tools.javac.Main RandomWriter.java
jar cf RandomWriter.jar RandomWriter*.class
