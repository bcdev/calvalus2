#!/bin/bash
CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

# Make sure that "${catalina.home}/conf/calvalus-reporting" is included in "common.loader" inside ${catalina.home}/conf/catalina.properties file.

sudo -u $RUN_AS mkdir -p $CATALINA_HOME/conf/calvalus-reporting
sudo -u $RUN_AS cp calvalus-reporting.properties $CATALINA_HOME/conf/calvalus-reporting
sudo -u $RUN_AS rm -rf $CATALINA_HOME/webapps/calvalus-reporting*
sudo -u $RUN_AS cp calvalus-reporting.war $CATALINA_HOME/webapps
