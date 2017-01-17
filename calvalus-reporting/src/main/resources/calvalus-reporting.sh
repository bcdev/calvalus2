#!/bin/bash

pwd=`pwd`
output=$1

java -cp "$pwd/generate-calvalus-report.jar:." com.bc.calvalus.generator.GenerateCalvalusReport start -o $output