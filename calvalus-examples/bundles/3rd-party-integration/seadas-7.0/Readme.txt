- download seadas 7.0
- install seadas 7.0
- install ocssw

python /opt/seadas/seadas-7.0/ocssw/run/scripts/install_ocssw.py --install-dir=/opt/seadas/seadas-7.0/ocssw --src --meris --aqua --seawifs

- create a bundle:
mkdir -p /opt/seadas/bundle/seadas-7.0
cd /opt/seadas/bundle/seadas-7.0
cp  ../../seadas-7.0/ocssw/OCSSW_bash.env .
cp -rv ../../seadas-7.0/ocssw/run  .
# remove git repos
find . -name .git -type d |xargs rm -rf
cd ..
tar czf seadas-7.0.tgz seadas-7.0



# installing / uninstalling

java -jar calvalus-distribution/target/cpt.jar --uninstall seadas-7.0
java -jar calvalus-distribution/target/cpt.jar -deploy calvalus-examples/bundles/3rd-party-integration/seadas-7.0/* seadas-7.0
