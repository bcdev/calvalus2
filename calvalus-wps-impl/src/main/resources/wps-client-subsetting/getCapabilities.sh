#!/bin/bash

# This is a script to send GetCapabilities request to Calvalus WPS. The response is displayed on the screen.
# Usage : getCapabilities.sh
# Example : getCapabilities.sh

function usage {
    echo "--------------------------"
    echo "USAGE: getCapabilities.sh"
    echo "--------------------------"
}

if [ "$1" = "-h" ] ; then
    usage
    exit 0
fi

read -p "Enter User Name: " WPS_USER

wget -q --user=$WPS_USER --ask-password -O- "www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetCapabilities"

