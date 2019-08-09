#!/bin/bash
CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo -u $RUN_AS cp calvalus-reporting.war $CATALINA_HOME/webapps
sudo -u $RUN_AS cp calvalus-reporting.properties $CATALINA_HOME/conf/calvalus
