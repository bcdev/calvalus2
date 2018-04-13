Several Calvalus WPS config files to be deployed in Tomcat:
1. calvalus.config : ${catalina.home}/conf/calvalus
2. log4j.properties : ${catalina.home}/conf/calvalus
3. calwpsL3Parameters-schema.xsd : ${catalina.home}/webapps/bc-wps

Make sure that "${catalina.home}/conf/calvalus" is included in "common.loader" inside ${catalina.home}/conf/catalina.properties file.