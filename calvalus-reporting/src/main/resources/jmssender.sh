#!/bin/bash

URL=$1
QUEUE_NAME=$2
FILE_PATH=$3
IS_BULK=$4

function error_exit
{
        echo
        echo "${1:-"Unknown Error"}" 1>&2
        echo
        echo "USAGE: $(basename $0) <message_consumer_url> <queue_name> <path_to_the_accounting_json_file> <optional: bulk>"
        echo
        exit 1
}

execResponse=$(exec java -cp "$REPORTING_INST/lib/calvalus-reporting.jar:$REPORTING_INST/etc" com.bc.calvalus.reporting.code.JmsSender $URL $QUEUE_NAME $FILE_PATH $IS_BULK)
response=$(echo "$?")

if [[ ${response} != 0  ]]; then
    error_exit "${execResponse}"
else
    echo
    echo "message(s) successfully sent"
    echo
fi