#!/bin/bash

#status comment 3073 15041 1057 1121
function handle_progress() {
  line=$1
  echo $line
  if [[ ${line} =~ status\ comment\ ([0-9]+)\ ([0-9]+)\ ([0-9]+)\ ([0-9]+) ]]; then
    a1=${BASH_REMATCH[1]}
    a2=${BASH_REMATCH[2]}
    progress=$(echo "scale=3; ${a1} / ${a2}" | bc)
    printf "CALVALUS_PROGRESS %.3f\n" $progress
  fi
}

while true; do
  sleep 20
  handle_progress "$(tail -n 5 status.txt|grep comment)"
done
 