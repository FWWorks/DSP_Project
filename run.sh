#!/bin/bash


hdfs dfs -rm -r output

hadoop jar task.jar WordCount input output

hdfs dfs -ls output
