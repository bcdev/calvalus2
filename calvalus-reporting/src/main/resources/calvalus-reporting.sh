#!/bin/bash

pwd=`pwd`
output=$1

nohup java -cp "$pwd/generate-calvalus-report.jar:." com.bc.calvalus.generator.GenerateCalvalusReport start -o $output > $(dirname $1)/calvalus-reporting.log &