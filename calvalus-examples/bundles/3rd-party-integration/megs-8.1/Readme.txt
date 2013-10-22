Create megs bundle from odesa package
======================================

tar xf odesa-1.2.4.tar.gz
mkdir megs-8.1
cp -r odesa-1.2.4/resources/auxdatafiles megs-8.1/
cp -r odesa-1.2.4/resources/processors/MEGS_8.1/files megs-8.1/resources
cp odesa-1.2.4/resources/processors/MEGS_8.1/bin/level2 megs-8.1/
cp odesa-1.2.4/resources/processors/MEGS_8.1/bin/index megs-8.1/
tar czf megs-8.1.tar.gz megs-8.1
