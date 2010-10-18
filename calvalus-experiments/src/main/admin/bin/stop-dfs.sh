#!/bin/bash

if [ $# = 0 ]; then
  echo "Enter password for user 'hadoop':"
  read -s password
elif [ $# = 1 ]; then
  password=$1
else
  echo "Usage: ${0} [Password]"
fi

slaves=`cat ~/conf/slaves | sed  "s/#.*$//;/^$/d"`
masters=`cat ~/conf/masters | sed  "s/#.*$//;/^$/d"`

##### stop HDFS
# stop namenode
echo "Stopping namenode..."
echo ${password} | sudo -S service hadoop-0.20-namenode stop

# stop all datanode
for slave in ${slaves}; do
  echo "Stopping datanode on ${slave}..."
  ssh ${slave} "echo ${password} | sudo -S service hadoop-0.20-datanode stop"
done

# stop secondary namenode
for master in ${masters}; do
  echo "Stopping secondarynamenode on ${master}..."
  ssh ${master} "echo ${password} | sudo -S service hadoop-0.20-secondarynamenode stop"
done

