package com.bc.calvalus.wpsrest;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

/**
 * Created by hans on 15/09/2015.
 */
public class CalvalusParameterTest {

    @Test
    public void canGetAllParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getAllParameters();

        assertThat(calvalusParameters.size(), equalTo(13));
        assertThat(calvalusParameters, hasItems("productionType"));
        assertThat(calvalusParameters, hasItems("calvalus.calvalus.bundle"));
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
        assertThat(calvalusParameters, hasItems("calvalus.output.format"));
    }

    @Test
    public void canGetProductionParameters() throws Exception {
        List<String> calvalusParameters = CalvalusParameter.getProductionInfoParameters();

        assertThat(calvalusParameters.size(), equalTo(4));
        assertThat(calvalusParameters, hasItems("productionType"));
        assertThat(calvalusParameters, hasItems("calvalus.calvalus.bundle"));
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
        assertThat(calvalusParameters, hasItems("calvalus.output.format"));
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
        assertThat(CalvalusParameter.CALVALUS_OUTPUT_FORMAT.getIdentifier(), equalTo("calvalus.output.format"));
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