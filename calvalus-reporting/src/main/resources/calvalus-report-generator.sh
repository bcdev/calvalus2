#!/bin/bash

pwd=`pwd`
output=$1

java -cp "$pwd/calvalus-report-generator.jar:." com.bc.calvalus.generator.GenerateCalvalusReport start -o $output