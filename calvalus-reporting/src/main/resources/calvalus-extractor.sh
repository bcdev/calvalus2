#!/bin/bash

pwd=`pwd`
output=$1

nohup java -cp "$pwd/calvalus-extractor.jar:." com.bc.calvalus.extractor.ExtractCalvalusReport start -o $output > $(dirname $1)/calvalus-reporting.log &