#!/bin/bash

# Shows all Java processes on the Hadoop cluster.

servers="$(cat ~/conf/masters ~/conf/slaves | sed 's/#.*$//;/^$/d')"

echo "Showing all Java processes on the Hadoop cluster"

for srv in $servers; do
  ssh $srv "jps" 2>&1 |grep -v Jps | sed "s/.* \([^ ]*\)/${srv}: \1/" &
done
wait
echo "Done."
