#!/bin/bash

# shows current time on all servers on the cluster

servers="$(cat ~/conf/masters ~/conf/slaves | sed 's/#.*$//;/^$/d')"

for srv in $servers; do
  ssh $srv "date" 2>&1 | sed "s/\(.*\)/${srv}: \1/" &
done

wait

echo "Done."
