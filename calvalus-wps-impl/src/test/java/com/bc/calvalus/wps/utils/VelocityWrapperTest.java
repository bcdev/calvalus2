package com.bc.calvalus.wps.utils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;

import org.apache.velocity.app.VelocityEngine;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author hans
 */
public class VelocityWrapperTest {

    @Test
    public void canMerge() throws Exception {
        VelocityWrapper wrapper = new VelocityWrapper();
        Map<String, Object> contextMap = new HashMap<>();
        contextMap.put("key1", "dummy1");
        contextMap.put("key2", "dummy2");
        contextMap.put("key3", "dummy3");
        String mergedString = wrapper.merge(contextMap, "test-velocity3.vm");
        String mergedStringClean = mergedString.replaceAll("\r", "");

        assertThat(mergedStringClean, equalTo("<?xml version=\"1.0\"?>\n" +
                                                 "\n<test testAttr=\"$test\">\n" +
                                                 "    <contents>\n" +
                                                 "        <key>dummy1</key>\n" +
                                                 "        <key>dummy2</key>\n" +
                                                 "        <key>dummy3</key>\n" +
                                                 "    </contents>\n" +
                                                 "</test>"));
    }

    @Test
    public void canInitWithDefault() throws Exception {
        VelocityWrapper wrapper = new VelocityWrapper();

        VelocityEngine velocityEngine = wrapper.getVelocityEngine();

//        don't know how to assert Vector object
//        assertThat(velocityEngine.getProperty("resource.loader"), equalTo("classpath"));
        assertThat(velocityEngine.getProperty("classpath.resource.loader.class"), equalTo("org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader"));
        assertThat(velocityEngine.getProperty("runtime.log.logsystem.class"), equalTo("org.apache.velocity.runtime.log.SimpleLog4JLogSystem"));
        assertThat(velocityEngine.getProperty("runtime.log.logsystem.log4j.category"), equalTo("velocity"));
        assertThat(velocityEngine.getProperty("runtime.log.logsystem.log4j.logger"), equalTo("velocity"));
    }

    @Test
    public void canInitWithProperty() throws Exception {
        Properties properties = new Properties();
        properties.put("property1", "value1");
        properties.put("property2", "value2");
        VelocityWrapper wrapper = new VelocityWrapper(properties);

        VelocityEngine velocityEngine = wrapper.getVelocityEngine();

        assertThat(velocityEngine.getProperty("property1"), equalTo("value1"));
        assertThat(velocityEngine.getProperty("property2"), equalTo("value2"));
    }
}