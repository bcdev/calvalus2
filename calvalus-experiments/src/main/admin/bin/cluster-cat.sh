#!/bin/bash

# cat's the same file from all servers on the cluster

servers="$(cat ~/conf/masters ~/conf/slaves | sed 's/#.*$//;/^$/d')"

for srv in $servers; do
  echo "============  ${srv}:${1}  ======================"
  ssh $srv "cat ${1}"
done

echo "Done."
