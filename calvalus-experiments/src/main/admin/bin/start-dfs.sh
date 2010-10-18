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

##### start HDFS
# start namenode
echo "Starting namenode..."
echo ${password} | sudo -S service hadoop-0.20-namenode start

# start all datanode
for slave in ${slaves}; do
  echo "Starting datanode on ${slave}..."
  ssh ${slave} "echo ${password} | sudo -S service hadoop-0.20-datanode start"
done

# start secondary namenode
for master in ${masters}; do
  echo "Starting secondarynamenode on ${master}..."
  ssh ${master} "echo ${password} | sudo -S service hadoop-0.20-secondarynamenode start"
done


