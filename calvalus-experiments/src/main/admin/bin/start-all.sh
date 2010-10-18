#!/bin/bash

if [ $# = 0 ]; then
  echo "Enter password for user 'hadoop':"
  read -s password
elif [ $# = 1 ]; then
  password=$1
else
  echo "Usage: ${0} [Password]"
fi

##### start HDFS
~/Admin/start-dfs.sh ${password}

##### start MapReduce
~/Admin/start-mapred.sh ${password}

