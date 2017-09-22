#!/bin/bash

CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo service bc-tomcat stop
sudo cp calvalus-rest.properties $CATALINA_HOME/conf/calvalus
sudo chown root:tomcat $CATALINA_HOME/conf/calvalus/calvalus-rest.properties
sudo -u $RUN_AS rm -rf $CATALINA_HOME/webapps/calvalus-rest*
sudo -u $RUN_AS cp calvalus-rest.war $CATALINA_HOME/webapps
sudo service bc-tomcat start
