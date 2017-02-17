#!/bin/bash

pwd=`pwd`
output=$1

nohup java -cp "$pwd/calvalus-report-generator.jar:." com.bc.calvalus.generator.GenerateCalvalusReport start -o $output > $(dirname $1)/calvalus-reporting.log &