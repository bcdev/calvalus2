#!/bin/bash

#export JAVAHOME=/usr/lib/jdk1.6.0_06/bin

#export JAVAEXE=$JAVAHOME/java
export LIBDIR=lib
export OLD_CLASSPATH=$CLASSPATH

export CLASSPATH=$LIBDIR/*

java -Xms64M -Xmx512M -classpath "$CLASSPATH" com.bc.calvalus.ftp.CalvalusFtpServer "$@"

export CLASSPATH=$OLD_CLASSPATH