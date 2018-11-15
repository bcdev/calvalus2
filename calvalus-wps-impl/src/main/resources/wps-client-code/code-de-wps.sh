#!/bin/bash

#__author__ = "Thomas Storm and Hans Permana, Brockmann Consult GmbH"
#__copyright__ = "Copyright 2018, Brockmann Consult GmbH"
#__license__ = "For use with Calvalus processing systems"
#__version__ = "1.0"
#__email__ = "info@brockmann-consult.de"
#__status__ = "Production"

# changes in 1.1
# ...

if [[ $1 == "h" || $1 == "" ]] ; then
  echo "Usage: code-de-wps.sh username password type <JobId|requestXml>"
  echo "    type MUST be one of GetCapabilities DescribeProcess Execute GetStatus FetchResults"
  echo "        GetCapabilities: Provides the capabilities of the CODE-DE WPS interface"
  echo "        DescribeProcess: Prints all usable processors of the CODE-DE processing system"
  echo "        Execute: Submits the given processing request to the CODE-DE processing system"
  echo "        GetStatus: Submits the given processing request to the CODE-DE processing system"
  echo "        FetchResult: Downloads the result of the given URL"
  echo "    if type is Execute, you MUST provide requestXml as 4th parameter"
  echo "    if type is GetStatus, you MUST provide JobId as 4th parameter"
  echo "    if type is FetchResult, you MUST provide a URL you got from GetStatus after successful processing as 4th parameter"
  exit 0
fi

#set -x

urlencode() {
  local string="${1}"
  local strlen=${#string}
  local encoded=""
  local pos c o

  for (( pos=0 ; pos<strlen ; pos++ )); do
     c=${string:$pos:1}
     case "$c" in
        [-_.~a-zA-Z0-9] ) o="${c}" ;;
        * )               printf -v o '%%%02x' "'$c"
     esac
     encoded+="${o}"
  done
  echo "${encoded}"    # You can either set a return variable (FASTER)
  REPLY="${encoded}"   #+or echo the result (EASIER)... or both... :p
}

CAS_HOSTNAME=sso.eoc.dlr.de
#CAS_HOSTNAME=tsedos.eoc.dlr.de
WPS_HOSTNAME=processing.code-de.org
#WPS_HOSTNAME=processing.code-de-ref.eoc.dlr.de

USERNAME=$1
PASSWORD=$2

SERVICE_TYPE=$3

COOKIE_JAR=.cookieJar
USER_AGENT=curl/7.29.0

if [[ ${SERVICE_TYPE} == "GetCapabilities" ]] ; then
    QUERY_STRING_CLEAR="?Service=WPS&Request=GetCapabilities"
elif [[ ${SERVICE_TYPE} == "DescribeProcess" ]] ; then
    QUERY_STRING_CLEAR="?Service=WPS&Request=DescribeProcess&Version=1.0.0&Identifier=all"
elif [[ ${SERVICE_TYPE} == "Execute" ]] ; then
    QUERY_STRING_CLEAR=""
elif [[ ${SERVICE_TYPE} == "GetStatus" ]] ; then
    QUERY_STRING_CLEAR="?Service=WPS&Request=GetStatus&JobId=$4"
fi

QUERY_STRING=$( urlencode ${QUERY_STRING_CLEAR} )

SERVICE_NAME_CLEAR=https://${WPS_HOSTNAME}/wps${QUERY_STRING_CLEAR}
SERVICE_NAME=$( urlencode ${SERVICE_NAME_CLEAR} )

rm -f ${COOKIE_JAR}

# todo - add code similar to this in order to re-use old but valid cookies
#if [[ $(find "$COOKIE_JAR" -mtime +1 -print) ]]; then
#    rm -f ${COOKIE_JAR}
#    loginResult="$(curl "https://tsedos.eoc.dlr.de/cas-codede/login?service=${SERVICE_NAME}" -u ${USERNAME}:${PASSWORD} -v -c ${COOKIE_JAR} 2>&1)"
#fi

loginResult="$(curl "https://${CAS_HOSTNAME}/cas-codede/login?service=${SERVICE_NAME}" -u ${USERNAME}:${PASSWORD} -v -c ${COOKIE_JAR} -A "$USER_AGENT" 2>&1)"

# Send the cookie to the WPS, and use it there

if [[ ${SERVICE_TYPE} == "Execute" ]] ; then
    COOKIE=$(cat ${COOKIE_JAR} | sed "s:.*CASTGC::" | sed "s:#.*::" | sed "s:#.*::" | sed "s:#.*::" | tr -d '[:space:]')
    curl ${SERVICE_NAME_CLEAR} -b ${COOKIE_JAR} -k -L -H "Cookie: requestId=$(uuidgen);CASTGC=${COOKIE}" -F "request=@test-request.xml" -A "$USER_AGENT"
elif [[ ${SERVICE_TYPE} == "GetStatus" ]] ; then
    curl ${SERVICE_NAME_CLEAR} -b ${COOKIE_JAR} -k -s -L -H "Cookie: queryString=${QUERY_STRING}" -A "$USER_AGENT"
elif [[ ${SERVICE_TYPE} == "FetchResult" ]] ; then
    downloadUrl=$(echo $4 | sed 's/http:/https:/' | sed 's/:80\//\//')
    curl ${downloadUrl} -c ${COOKIE_JAR} -b ${COOKIE_JAR} -k -O -L -s -A "$USER_AGENT"
    curl ${downloadUrl} -b ${COOKIE_JAR} -k -O -L -A "$USER_AGENT"
else
    curl ${SERVICE_NAME_CLEAR} -b ${COOKIE_JAR} -k -L -H "Cookie: queryString=${QUERY_STRING}" -A "$USER_AGENT" 
fi

