#!/bin/bash

URL=$1
QUEUE_NAME=$2
FILE_PATH=$3

exec java -cp "$CALVALUS_INST/lib/calvalus-reporting.jar" com.bc.calvalus.reporting.code.JmsSender $URL $QUEUE_NAME $FILE_PATH