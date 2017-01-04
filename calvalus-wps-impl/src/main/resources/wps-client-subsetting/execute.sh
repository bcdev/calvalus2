#!/bin/bash

# This is a script to send an execute request to Calvalus WPS with a given xml request file
# Usage : execute.sh [execute request XML] [response XML file]
# Example : execute.sh calvalus-wps-Execute-request.xml response.xml

function usage {
    echo "---------------------------------------------------------"
    echo "USAGE: execute.sh <execute_request_file> <response_file>"
    echo "---------------------------------------------------------"
}

if [ "$1" = "-h" ] ; then
    usage
    exit 0
elif [ -z "$1" ] ; then
    echo "the request XML file is missing"
    usage
    exit 0
elif [ -z "$2" ] ; then
    echo "the response file has not been specified"
    usage
    exit 0
fi

EXECUTE_XML=$1
RESPONSE_FILE=$2

read -p "Enter User Name: " WPS_USER

wget --user=$WPS_USER --ask-password --header="Content-Type:application/xml" --post-file="${EXECUTE_XML}" -O "${RESPONSE_FILE}" "www.brockmann-consult.de/bc-wps/wps/calvalus"
