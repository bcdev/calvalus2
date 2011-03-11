#!/bin/bash

baseDir="`dirname ${0}`/.."
baseDir=`( cd $baseDir ; pwd )`

hadoop fs -rm /calvalus/software/0.5/beam-4.9-SNAPSHOT/*.jar
hadoop fs -copyFromLocal ${baseDir}/lib/beam/*.jar /calvalus/software/0.5/beam-4.9-SNAPSHOT/
hadoop fs -ls /calvalus/software/0.5/beam-4.9-SNAPSHOT/
echo "=================="
hadoop fs -rm /calvalus/software/0.5/calvalus-1.0-SNAPSHOT/*.jar
hadoop fs -copyFromLocal ${baseDir}/lib/lib/*.jar /calvalus/software/0.5/calvalus-1.0-SNAPSHOT/
hadoop fs -ls /calvalus/software/0.5/calvalus-1.0-SNAPSHOT/