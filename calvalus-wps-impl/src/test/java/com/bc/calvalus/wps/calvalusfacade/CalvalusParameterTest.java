package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.*;

import org.junit.*;

import java.util.List;

/**
 * @author hans
 */
public class CalvalusParameterTest {

    @Test
    public void canGetAllParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getAllParameters();

        assertThat(calvalusParameters.size(), equalTo(14));
        assertThat(calvalusParameters, hasItems("productionType"));
        assertThat(calvalusParameters, hasItems("calvalus.calvalus.bundle"));
        assertThat(calvalusParameters, hasItems("calvalus.beam.bundle"));
        assertThat(calvalusParameters, hasItems("calvalus.snap.bundle"));
        assertThat(calvalusParameters, hasItems("productionName"));
        assertThat(calvalusParameters, hasItems("processorBundleName"));
        assertThat(calvalusParameters, hasItems("processorBundleVersion"));
        assertThat(calvalusParameters, hasItems("processorName"));
        assertThat(calvalusParameters, hasItems("inputDataSetName"));
        assertThat(calvalusParameters, hasItems("minDate"));
        assertThat(calvalusParameters, hasItems("maxDate"));
        assertThat(calvalusParameters, hasItems("periodLength"));
        assertThat(calvalusParameters, hasItems("regionWKT"));
        assertThat(calvalusParameters, hasItems("outputFormat"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getProductionInfoParameters();

        assertThat(calvalusParameters.size(), equalTo(5));
        assertThat(calvalusParameters, hasItems("productionType"));
        assertThat(calvalusParameters, hasItems("calvalus.calvalus.bundle"));
        assertThat(calvalusParameters, hasItems("calvalus.beam.bundle"));
        assertThat(calvalusParameters, hasItems("calvalus.snap.bundle"));
        assertThat(calvalusParameters, hasItems("productionName"));
    }

    @Test
    public void canGetProductSetParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getProductsetParameters();

        assertThat(calvalusParameters.size(), equalTo(5));
        assertThat(calvalusParameters, hasItems("minDate"));
        assertThat(calvalusParameters, hasItems("maxDate"));
        assertThat(calvalusParameters, hasItems("periodLength"));
        assertThat(calvalusParameters, hasItems("regionWKT"));
        assertThat(calvalusParameters, hasItems("outputFormat"));
    }

    @Test
    public void canGetProcessorInfoParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getProcessorInfoParameters();

        assertThat(calvalusParameters.size(), equalTo(3));
        assertThat(calvalusParameters, hasItems("processorBundleName"));
        assertThat(calvalusParameters, hasItems("processorBundleVersion"));
        assertThat(calvalusParameters, hasItems("processorName"));
    }

    @Test
    public void canGetParameterId() throws Exception {
        assertThat(CalvalusParameter.CALVALUS_OUTPUT_FORMAT.getIdentifier(), equalTo("outputFormat"));
    }

    @Test
    public void canGetAbstractText() throws Exception {
        assertThat(CalvalusParameter.SNAP_BUNDLE_VERSION.getAbstractText(), equalTo("SNAP bundle version"));
    }

    @Test
    public void canGetParameterType() throws Exception {
        assertThat(CalvalusParameter.REGION_WKT.getType(), equalTo("productSet"));
    }

}