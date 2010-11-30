#!/bin/sh

runtimeplotterdir=`dirname $0`/../../calvalus-reporting/runtime-plotter
classpath="$runtimeplotterdir/target/runtime-plotter-0.1-SNAPSHOT.jar:$runtimeplotterdir/target/lib/bcmail-jdk14-1.38.jar:$runtimeplotterdir/target/lib/bcmail-jdk14-138.jar:$runtimeplotterdir/target/lib/bcprov-jdk14-1.38.jar:$runtimeplotterdir/target/lib/bcprov-jdk14-138.jar:$runtimeplotterdir/target/lib/bctsp-jdk14-1.38.jar:$runtimeplotterdir/target/lib/itext-2.1.7.jar:$runtimeplotterdir/target/lib/jcommon-1.0.15.jar:$runtimeplotterdir/target/lib/jfreechart-1.0.13.jar"

#java -Xmx2g -classpath $classpath com.bc.calvalus.plot.ChartPlotter $*

java -Xmx2g -classpath $classpath com.bc.calvalus.plot.runtime.RuntimePlotter $*
