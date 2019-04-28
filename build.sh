#!/bin/bash

export HADOOP_CLASSPATH=$(/usr/local/hadoop/bin/hadoop classpath)

javac -classpath ${HADOOP_CLASSPATH} -d build/ src/*.java

jar -cvf task.jar -C build/ .

