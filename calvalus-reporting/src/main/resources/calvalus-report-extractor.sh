#!/bin/bash

pwd=`pwd`
output=$1

nohup java -cp "$pwd/calvalus-report-extractor.jar:." com.bc.calvalus.generator.ExtractCalvalusReport start -o $output > $(dirname $1)/calvalus-reporting.log &