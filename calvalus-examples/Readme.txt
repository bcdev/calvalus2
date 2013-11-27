Calvalus examples
==================
This module contains various examples demonstrating the capabilities of Calvalus.


alias cpt ='java -jar ~/projects/calvalus/calvalus-distribution/target/cpt.jar'
cd ~/projects/calvalus/calvalus-examples


Example deployment
==================
The bundles for the examples in this module must be deployed with the CPT
to a Calvalus cluster before being used by one of the supplied requests.


cpt --install bundles/r/examples-r-1.0
cpt --install bundles/examples-python-1.0
cpt --install bundles/examples-shell-1.0


3rd Party Bundles
==================
The bundles in the 3rd-party-integration directory require
one or more binary archives (*.zip or *.tar.gz) which contain the
processor itself.


Example execution
==================
The supplied request can be executed using the CPT.

cpt requests/r/rcopy.xml
cpt requests/python/pythoncopy.xml
cpt requests/shell/00_simple.xml

For the matchup use case the point files first has be copied to HDFS:
hadoop fs -put requests/matchup/cities.txt hdfs://master00:9000/calvalus/examples/cities.txt
cpt requests/matchup/00_cities.xml



Other bundles can be found in the following projects:
======================================================
oc-cci: https://github.com/bcdev/oc-cci/tree/master/calvalus-bundles
siocs:  https://github.com/bcdev/siocs/tree/master/momo-calvalus-adapter
