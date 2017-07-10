#!/bin/bash
REPORTING_HOME=/home/cvop/reporting-inst
SERVICE_NAME=bc-tomcat
RUN_AS=tomcat

sudo -u $RUN_AS cp calvalus-reporting.jar $REPORTING_HOME/lib
sudo -u $RUN_AS cp conf.xsl $REPORTING_HOME/etc
sudo -u $RUN_AS cp counter.xsl $REPORTING_HOME/etc
sudo -u $RUN_AS cp reporting-collector.properties $REPORTING_HOME/etc
