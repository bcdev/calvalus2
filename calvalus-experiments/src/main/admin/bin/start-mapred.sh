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

##### start MapReduce
# start jobtracker
echo "Starting jobtracker..."
echo ${password} | sudo -S service hadoop-0.20-jobtracker start

# start all tasktracker
for slave in ${slaves}; do
  echo "Starting tasktracker on ${slave}..."
  ssh ${slave} "echo ${password} | sudo -S service hadoop-0.20-tasktracker start"
done

