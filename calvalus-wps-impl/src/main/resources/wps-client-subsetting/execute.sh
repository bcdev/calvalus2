#!/bin/sh

# This is a script to send an execute request to Calvalus WPS with a given xml request file
# Usage : execute.sh [execute request XML] [response XML file]
# Example : execute.sh calvalus-wps-Execute-request.xml response.xml

EXECUTE_XML=$1
RESPONSE_FILE=$2

read -p "Enter User Name: " WPS_USER

wget --user=$WPS_USER --ask-password --header="Content-Type:application/xml" --post-file="${EXECUTE_XML}" -O "${RESPONSE_FILE}" "www.brockmann-consult.de/bc-wps/wps/calvalus"
