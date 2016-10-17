package com.bc.calvalus.wps.utils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;
import org.junit.*;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
public class ProductMetadataTest {

    private static final long START_DATE = 946684800000L;
    private static final long END_DATE = 1577836800000L;
    private Production mockProduction;
    private WpsServerContext mockServerContext;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        configureMockProduction();
        configureMockServerContext();
    }

    @Test
    public void testUriBuilder() throws Exception {
        UriBuilder builder = new UriBuilderImpl();
        builder.scheme("http")
                    .host("bc-wps")
                    .port(9080)
                    .path("bc-wps")
                    .path("staging")
                    .path("TEP Subset-metadata");
        assertThat(builder.build().toString(), equalTo("http://bc-wps:9080/bc-wps/staging/TEP%20Subset-metadata"));
    }

    @Test
    public void canCreateProductMetadata() throws Exception {
        List<File> mockProductionFileList = new ArrayList<>();
        File mockFile1 = getMockFile("result1.nc", 1000000000L);
        mockProductionFileList.add(mockFile1);
        File mockFile2 = getMockFile("result.zip", 1000000L);
        mockProductionFileList.add(mockFile2);
        File mockFile3 = getMockFile("result-metadata", 5000L);
        mockProductionFileList.add(mockFile3);
        File mockFile4 = getMockFile("result.xml", 1000L);
        mockProductionFileList.add(mockFile4);

        ProductMetadata productMetadata = ProductMetadataBuilder.create()
                    .withProduction(mockProduction)
                    .withProductionResults(mockProductionFileList)
                    .withServerContext(mockServerContext)
                    .build();

        Map<String, Object> contextMap = productMetadata.getContextMap();
        assertThat(contextMap.get("jobFinishTime"), equalTo("2016-01-01T01:00:00.000+01:00"));
        assertThat(contextMap.get("productOutputDir"), equalTo("user/20160317_10000000"));
        assertThat(contextMap.get("productionName"), equalTo("TEP Subset test"));
        assertThat(contextMap.get("processName"), equalTo("Subset"));
        assertThat(contextMap.get("inputDatasetName"), equalTo("Urban Footprint Global (Urban TEP)"));
        assertThat(contextMap.get("regionWkt"), equalTo("-10 100 0 100 0 110 -10 110 -10 100"));
        assertThat(contextMap.get("startDate"), equalTo("2000-01-01T01:00:00.000+01:00"));
        assertThat(contextMap.get("stopDate"), equalTo("2020-01-01T01:00:00.000+01:00"));
        assertThat(contextMap.get("collectionUrl"), equalTo("http://www.brockmann-consult.de:80/bc-wps/staging/user/20160317_10000000"));
        assertThat(contextMap.get("processorVersion"), equalTo("1.0"));
        assertThat(contextMap.get("productionType"), equalTo("2"));
        assertThat(contextMap.get("outputFormat"), equalTo("NetCDF4"));

        List productionList = (List) contextMap.get("productList");
        assertThat(productionList.size(), equalTo(4));
        Map product1 = (Map) productionList.get(0);
        assertThat(product1.get("productFileName"), equalTo("result1.nc"));
        assertThat(product1.get("productFileFormat"), equalTo("NetCDF4"));
        assertThat(product1.get("productFileSize"), equalTo("1000000000"));
        assertThat(product1.get("productUrl"), equalTo("http://www.brockmann-consult.de:80/bc-wps/staging/user/20160317_10000000/result1.nc"));

        Map product2 = (Map) productionList.get(1);
        assertThat(product2.get("productFileName"), equalTo("result.zip"));
        assertThat(product2.get("productFileFormat"), equalTo("ZIP"));
        assertThat(product2.get("productFileSize"), equalTo("1000000"));
        assertThat(product2.get("productUrl"), equalTo("http://www.brockmann-consult.de:80/bc-wps/staging/user/20160317_10000000/result.zip"));

        Map product3 = (Map) productionList.get(2);
        assertThat(product3.get("productFileName"), equalTo("result-metadata"));
        assertThat(product3.get("productFileFormat"), equalTo("metadata"));
        assertThat(product3.get("productFileSize"), equalTo("5000"));
        assertThat(product3.get("productUrl"), equalTo("http://www.brockmann-consult.de:80/bc-wps/staging/user/20160317_10000000/result-metadata"));

        Map product4 = (Map) productionList.get(3);
        assertThat(product4.get("productFileName"), equalTo("result.xml"));
        assertThat(product4.get("productFileFormat"), equalTo("XML"));
        assertThat(product4.get("productFileSize"), equalTo("1000"));
        assertThat(product4.get("productUrl"), equalTo("http://www.brockmann-consult.de:80/bc-wps/staging/user/20160317_10000000/result.xml"));
    }

    @Test
    public void name() throws Exception {
        String sample = "/opt/tomcat/webapps/bc-wps/staging/sysUser/remoteUser/stagingDir/file.nc";
//        String sample = "C:\\opt\\tomcat\\webapps\\bc-wps\\staging\\sysUser\\remoteUser\\stagingDir\\file.nc";
        String[] samplePaths = sample.split("[/\\\\]staging[/\\\\]");

        for (String singlePath : samplePaths) {
            System.out.println(singlePath);
        }

    }

    private File getMockFile(String name, long size) {
        File mockFile1 = mock(File.class);
        when(mockFile1.getName()).thenReturn(name);
        when(mockFile1.length()).thenReturn(size);
        return mockFile1;
    }

    private void configureMockProduction() throws ProductionException {
        mockProduction = mock(Production.class);

        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getString("processorName")).thenReturn("Subset");
        when(mockProductionRequest.getString("inputDataSetName")).thenReturn("Urban Footprint Global (Urban TEP)");
        when(mockProductionRequest.getString("regionWKT")).thenReturn("POLYGON((100 -10,100 0,110 0,110 -10,100 -10))");
        when(mockProductionRequest.getString("processorBundleVersion")).thenReturn("1.0");
        when(mockProductionRequest.getString("productionType")).thenReturn("L2Plus");
        when(mockProductionRequest.getString("outputFormat")).thenReturn("NetCDF4");
        when(mockProductionRequest.getDate("minDate")).thenReturn(new Date(START_DATE));
        when(mockProductionRequest.getDate("maxDate")).thenReturn(new Date(END_DATE));
        when(mockProductionRequest.getStagingDirectory(anyString())).thenReturn("user/20160317_10000000");
        DateRange mockDateRange = new DateRange(new Date(START_DATE), new Date(END_DATE));
        when(mockProductionRequest.createFromMinMax()).thenReturn(mockDateRange);
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);

        when(mockProduction.getName()).thenReturn("TEP Subset test");
        when(mockProduction.getId()).thenReturn("20160429140605_L2Plus_16bd8f26b258fc");
        when(mockProduction.getStagingPath()).thenReturn("user/20160317_10000000");

        WorkflowItem mockWorkflowItem = mock(WorkflowItem.class);
        when(mockWorkflowItem.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflowItem);
    }

    private void configureMockServerContext() {
        mockServerContext = mock(WpsServerContext.class);
        when(mockServerContext.getHostAddress()).thenReturn("www.brockmann-consult.de");
        when(mockServerContext.getPort()).thenReturn(80);
        when(mockServerContext.getRequestUrl()).thenReturn("http://www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetStatus&JobId=20160429140605_L2Plus_16bd8f26b258fc");
    }
}