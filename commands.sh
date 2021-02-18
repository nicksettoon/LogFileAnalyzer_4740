#! /bin/bash

dir="$1"
srcdir="weblog"
jar="lastrequest.jar"

## hdfs dfs -mkdir "$dir" 2> /dev/null
gunzip -c access_log.gz | hdfs dfs -put - weblog/access_log

echo "processing log files in $srcdir"
echo "output is in $dir"
hadoop jar $jar solution.LogFileAnalyzer $srcdir $dir
