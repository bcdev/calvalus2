#!/bin/bash

output=$1

nohup java -cp "$CV_REPORT_EXTRACT_INST:." com.bc.calvalus.extractor.ExtractCalvalusReport start -o $output > $(dirname $1)/calvalus-reporting.log &