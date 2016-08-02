#!/bin/bash

# This is a script to inquire a status of a process to Calvalus WPS with a given execute response XML file. The response of this request
# will be displayed on the screen. When the process has been successfully completed, the result URL is also provided in the response.
# Usage : getStatus.sh [execute response XML file]
# Example : getStatus.sh response.xml

RESPONSE_FILE=$1

GET_STATUS_URL=`grep "statusLocation=" "${RESPONSE_FILE}" | cut -d'"' -f4`

STATUS_URL_FORMATTED=`echo ${GET_STATUS_URL} | sed -e "s/\&amp;/\&/g"`

read -p "Enter User Name: " WPS_USER

wget -q --user=$WPS_USER --ask-password -O- "${STATUS_URL_FORMATTED}"
