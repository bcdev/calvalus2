#!/bin/bash
CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo rm -rf $CATALINA_HOME/webapps/calvalus-reporting-ui*
sudo -u $RUN_AS cp calvalus-reporting-ui.war $CATALINA_HOME/webapps
