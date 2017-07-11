#!/bin/bash

exec java -cp "$CALVALUS_INST/lib/calvalus-reporting.jar:$CALVALUS_INST/etc" com.bc.calvalus.reporting.collector.ReportingCollector