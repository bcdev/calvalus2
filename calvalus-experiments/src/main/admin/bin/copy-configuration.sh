#!/bin/bash

if [ ! -d ~/conf ]; then
  echo "configuration directory does not exist"
  exit 1
fi

slaves=`cat ~/conf/slaves | sed  "s/#.*$//;/^$/d"`

echo "Creating configuration directory..."
for slave in $slaves; do
  (ssh ${slave} "mkdir -p conf" && echo ${slave})&
done
wait

echo "Copying configuration..."
for slave in $slaves; do
  (scp -q ~/conf/*  ${slave}:~/conf/  && echo ${slave})&
done
wait
echo "Done."

