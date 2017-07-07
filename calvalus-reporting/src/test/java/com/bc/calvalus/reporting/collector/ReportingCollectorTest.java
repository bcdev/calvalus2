package com.bc.calvalus.reporting.collector;

import org.junit.*;

/**
 * @author hans
 */
public class ReportingCollectorTest {

    @Ignore // running this test is dependent on having connection to the history server
    @Test
    public void testMain() throws Exception {
        ReportingCollector.main(new String[]{"reporting-collector-test.properties"});
    }
}