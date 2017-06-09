#!/bin/bash

CATALINA_HOME=/opt/tomcat
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo service bc-tomcat stop
sudo -u $RUN_AS mkdir -p $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp calvalus.config $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp calvalus-wps.properties $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp log4j.properties $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp metadata-template.vm $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS cp -r local-process-descriptor $CATALINA_HOME/conf/calvalus
sudo -u $RUN_AS ln -sf /tank/calvalus/wps/staging $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS ln -sf /tank/calvalus/wps/staging/static-content/software $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS ln -sf /tank/calvalus/wps/staging/static-content/eodata $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS cp staging/directory-listing.xsl $CATALINA_HOME/webapps/bc-wps/staging
sudo -u $RUN_AS cp staging/directory-listing-readme.txt $CATALINA_HOME/webapps/bc-wps/staging
sudo -u $RUN_AS cp xsd/calwpsL3Parameters-schema.xsd $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS cp xsd/calwpsSpatioTemporalAggregationParameters-schema.xsd $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS cp *.jar $CATALINA_HOME/webapps/bc-wps/WEB-INF/lib
sudo -u $RUN_AS cp -r urbantep-quicklooks $CATALINA_HOME/webapps/ROOT
# don't forget to allow sym link in Tomcat directory.
# How-to: http://stackoverflow.com/questions/22240776/symlinking-tomcat-8-directory-resources
sudo -u $RUN_AS ln -sf ../ROOT/urbantep-quicklooks $CATALINA_HOME/webapps/bc-wps
sudo -u $RUN_AS ln -sf /tank/calvalus/wps/utep_input $CATALINA_HOME/webapps/bc-wps
sudo service bc-tomcat start
