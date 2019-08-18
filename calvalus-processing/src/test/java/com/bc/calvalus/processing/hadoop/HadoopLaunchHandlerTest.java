package com.bc.calvalus.processing.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * TODO add API doc
 *
 * @author Martin Boettcher
 */
public class HadoopLaunchHandlerTest {

    @Ignore
    @Test
    public void testRetrieveNoOfRunningTasks() throws ParserConfigurationException, SAXException, XPathExpressionException, IOException {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.xxx.master", "80.158.5.3");
        assertEquals("", 0, new HadoopLaunchHandler(configuration).retrieveNoOfRunningTasks());
    }

    @Ignore
    @Test
    public void testStartCluster() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.openstack.workdir", "/home/martin/tmp/dias-inst");
        configuration.set("calvalus.openstack.startcmd", "./cdt.py mundi startup s2-c2rcc-test-request.json");
        new HadoopLaunchHandler(configuration).startCluster();
        assertEquals("launched master", "80.158.", configuration.get("calvalus.hadoop.yarn.resourcemanager.hostname").substring(0, "80.158.".length()));
    }

    @Ignore
    @Test
    public void testStopCluster() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("calvalus.openstack.workdir", "/home/martin/tmp/dias-inst");
        configuration.set("calvalus.openstack.startcmd", "./cdt.py mundi startup s2-c2rcc-test-request.json");
        new HadoopLaunchHandler(configuration).stopCluster();
        assertEquals("launched master", null, configuration.get("calvalus.hadoop.yarn.resourcemanager.hostname"));
    }

}