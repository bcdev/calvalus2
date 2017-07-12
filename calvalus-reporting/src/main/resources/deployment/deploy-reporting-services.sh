#!/bin/bash
REPORTING_HOME=/home/cvop/reporting-inst
RUN_AS=cvop

sudo -u $RUN_AS cp calvalus-reporting.jar $REPORTING_HOME/lib
sudo -u $RUN_AS cp conf.xsl $REPORTING_HOME/etc
sudo -u $RUN_AS cp counters.xsl $REPORTING_HOME/etc
sudo -u $RUN_AS cp collector.sh $REPORTING_HOME
sudo -u $RUN_AS cp urbantep.sh $REPORTING_HOME
sudo -u $RUN_AS cp reporting-collector.properties $REPORTING_HOME/etc
sudo -u $RUN_AS cp urbantep.properties $REPORTING_HOME/etc
