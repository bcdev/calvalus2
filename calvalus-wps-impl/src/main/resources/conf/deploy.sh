#!/bin/bash

CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo service bc-tomcat stop
sudo -u $RUN_AS mkdir -p $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp calvalus.config $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp calvalus-wps.properties $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp log4j.properties $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS mkdir -p $CATALINA_HOME/webapps/bc-wps/staging
sudo -u $RUN_AS cp staging/directory-listing.xsl $CATALINA_HOME/webapps/bc-wps/staging
sudo -u $RUN_AS cp staging/directory-listing-readme.txt $CATALINA_HOME/webapps/bc-wps/staging
sudo -u $RUN_AS cp xsd/calwpsL3Parameters-schema.xsd $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS cp *.jar $CATALINA_HOME/webapps/bc-wps/WEB-INF/lib
sudo -u $RUN_AS cp -r urbantep-quicklooks $CATALINA_HOME/webapps/ROOT
sudo -u $RUN_AS ln -sf ../ROOT/urbantep-quicklooks $CATALINA_HOME/webapps/bc-wps/urbantep-quicklooks
sudo service bc-tomcat start
