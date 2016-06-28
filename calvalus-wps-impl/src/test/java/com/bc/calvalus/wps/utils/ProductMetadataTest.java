package com.bc.calvalus.wps.utils;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.junit.*;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class ProductMetadataTest {

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
        System.out.println("builder.toString() = " + builder.build().toString());
    }

    @Test
    public void canCreateVelocityContext() throws Exception {
        List<File> mockProductionResults = getMockProductionResults();
        ProductMetadata productMetadata = new ProductMetadata(mockProduction, mockProductionResults, mockServerContext);

        VelocityEngine ve = getVelocityEngine();
        VelocityContext context = productMetadata.createVelocityContext();
        Template template = ve.getTemplate("test-velocity2.vm");
        StringWriter writer = new StringWriter();
        template.merge(context, writer);

        assertThat(writer.toString(),
                   equalTo("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<feed xmlns=\"http://www.w3.org/2005/Atom\" xml:lang=\"en\">\n    <title type=\"text\">Urban TEP catalogue entry</title>\n    <id>http://www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetStatus&JobId=20160429140605_L2Plus_16bd8f26b258fc</id>\n    <updated>2016-01-01T01:00:00.000+01:00</updated>\n    <link rel=\"profile\" href=\"http://www.opengis.net/spec/owc-atom/1.0/req/core\" title=\"This file is compliant with version 1.0 of OGC Context\"/>\n    <entry>\n        <id>TEP Subset test/hans/20160317_10000000</id>\n        <title type=\"text\">TEP Subset test</title>\n        <content type=\"text\">Subset of /calvalus/auxiliary/urban-footprint/ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.tif</content>\n        <author>\n            <name>Brockmann Consult GmbH</name>\n        </author>\n        <publisher xmlns=\"http://purl.org/dc/elements/1.1/\">ESA Urban TEP</publisher>\n        <updated>2016-01-01T01:00:00.000+01:00</updated>\n        <parentIdentifier xmlns=\"http://purl.org/dc/elements/1.1/\">http://http://www.brockmann-consult.de:80/bc-wps/staging/hans</parentIdentifier>\n        <where xmlns=\"http://www.georss.org/georss/10\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n            <Polygon xmlns=\"http://www.opengis.net/gml\">\n                <exterior>\n                    <LinearRing>\n                        <posList srsDimension=\"2\">100 -10 100 0 110 0 110 -10 100 -10</posList>\n                    </LinearRing>\n                </exterior>\n            </Polygon>\n        </where>\n        <date xmlns=\"http://purl.org/dc/elements/1.1/\">DUMMY/DUMMY</date>\n        <link rel=\"enclosure\" href=\"http://http://www.brockmann-consult.de:80/bc-wps/staging/null\" title=\"TEP Subset test\"/>\n        <EarthObservation xmlns=\"http://www.opengis.net/sar/2.1\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n            <boundedBy xmlns=\"http://www.opengis.net/gml/3.2\" xsi:nil=\"true\"/>\n            <phenomenonTime xmlns=\"http://www.opengis.net/om/2.0\">\n                <TimePeriod xmlns=\"http://www.opengis.net/gml/3.2\">\n                    <beginPosition>DUMMY</beginPosition>\n                    <endPosition>DUMMY</endPosition>\n                </TimePeriod>\n            </phenomenonTime>\n            <observedProperty xmlns=\"http://www.opengis.net/om/2.0\" xsi:nil=\"true\"/>\n            <metaDataProperty xmlns=\"http://www.opengis.net/eop/2.1\">\n                <EarthObservationMetaData>\n                    <identifier>TEP Subset test/hans/20160317_10000000</identifier>\n                    <parentIdentifier>http://http://www.brockmann-consult.de:80/bc-wps/staging/hans</parentIdentifier>\n                    <productType>GUF</productType>\n                    <processing>\n                        <ProcessingInformation>\n                            <processingCenter>Brockmann Consult GmbH</processingCenter>\n                            <processingDate>2016-01-01T01:00:00.000+01:00</processingDate>\n                            <method>Subset Processing</method>\n                            <processorName>Subset</processorName>\n                            <processorVersion>1.0</processorVersion>\n                            <processingLevel>L2Plus</processingLevel>\n                            <nativeProductFormat>NetCDF4</nativeProductFormat>\n                        </ProcessingInformation>\n                    </processing>\n                </EarthObservationMetaData>\n            </metaDataProperty>\n            <result xmlns=\"http://www.opengis.net/om/2.0\">\n                <EarthObservationResult xmlns=\"http://www.opengis.net/eop/2.1\">\n                    <boundedBy xmlns=\"http://www.opengis.net/gml/3.2\" xsi:nil=\"true\"/>\n                                        <product>\n                        <ProductInformation>\n                            <fileName>\n                                <d7p1:ServiceReference d7p1:type=\"simple\"\n                                                       d7p2:href=\"http://http://www.brockmann-consult.de:80/bc-wps/staging/L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.nc\"\n                                                       d7p2:title=\"simple\"\n                                                       xmlns=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p1=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p2=\"http://www.w3.org/1999/xlink\">\n                                    <d7p1:Identifier>L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.nc</d7p1:Identifier>\n                                    <d7p1:Format>NetCDF4</d7p1:Format>\n                                </d7p1:ServiceReference>\n                            </fileName>\n                            <size>12345</size>\n                        </ProductInformation>\n                    </product>\n                                        <product>\n                        <ProductInformation>\n                            <fileName>\n                                <d7p1:ServiceReference d7p1:type=\"simple\"\n                                                       d7p2:href=\"http://http://www.brockmann-consult.de:80/bc-wps/staging/L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.zip\"\n                                                       d7p2:title=\"simple\"\n                                                       xmlns=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p1=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p2=\"http://www.w3.org/1999/xlink\">\n                                    <d7p1:Identifier>L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.zip</d7p1:Identifier>\n                                    <d7p1:Format>ZIP</d7p1:Format>\n                                </d7p1:ServiceReference>\n                            </fileName>\n                            <size>1234</size>\n                        </ProductInformation>\n                    </product>\n                                        <product>\n                        <ProductInformation>\n                            <fileName>\n                                <d7p1:ServiceReference d7p1:type=\"simple\"\n                                                       d7p2:href=\"http://http://www.brockmann-consult.de:80/bc-wps/staging/output-metadata.xml\"\n                                                       d7p2:title=\"simple\"\n                                                       xmlns=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p1=\"http://www.opengis.net/ows/2.0\"\n                                                       xmlns:d7p2=\"http://www.w3.org/1999/xlink\">\n                                    <d7p1:Identifier>output-metadata.xml</d7p1:Identifier>\n                                    <d7p1:Format>XML</d7p1:Format>\n                                </d7p1:ServiceReference>\n                            </fileName>\n                            <size>34</size>\n                        </ProductInformation>\n                    </product>\n                                        <browse>\n                        <BrowseInformation>\n                            <fileName>$product.productQuickLookUrl</fileName>\n                        </BrowseInformation>\n                    </browse>\n                </EarthObservationResult>\n            </result>\n            <featureOfInterest xmlns=\"http://www.opengis.net/om/2.0\">\n                <Footprint xmlns=\"http://www.opengis.net/eop/2.1\">\n                    <boundedBy xmlns=\"http://www.opengis.net/gml/3.2\" xsi:nil=\"true\"/>\n                    <multiExtentOf>\n                        <MultiSurface xmlns=\"http://www.opengis.net/gml/3.2\">\n                            <surfaceMembers>\n                                <Polygon>\n                                    <exterior>\n                                        <LinearRing>\n                                            <posList count=\"5\" srsDimension=\"2\">100 -10 100 0 110 0 110 -10 100 -10</posList>\n                                        </LinearRing>\n                                    </exterior>\n                                </Polygon>\n                            </surfaceMembers>\n                        </MultiSurface>\n                    </multiExtentOf>\n                </Footprint>\n            </featureOfInterest>\n        </EarthObservation>\n    </entry>\n</feed>\n"));
    }

    private VelocityEngine getVelocityEngine() {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();
        return ve;
    }

    private List<File> getMockProductionResults() {
        List<File> mockProductionResults = new ArrayList<>();
        File mockResultFile = mock(File.class);
        when(mockResultFile.getName()).thenReturn("L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.nc");
        when(mockResultFile.length()).thenReturn(12345L);
        mockProductionResults.add(mockResultFile);

        File mockResultZipFile = mock(File.class);
        when(mockResultZipFile.getName()).thenReturn("L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.zip");
        when(mockResultZipFile.length()).thenReturn(1234L);
        mockProductionResults.add(mockResultZipFile);

        File mockMetadataFile = mock(File.class);
        when(mockMetadataFile.getName()).thenReturn("output-metadata.xml");
        when(mockMetadataFile.length()).thenReturn(34L);
        mockProductionResults.add(mockMetadataFile);
        return mockProductionResults;
    }

    private void configureMockProduction() throws ProductionException {
        mockProduction = mock(Production.class);

        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getString("processorName")).thenReturn("Subset");
        when(mockProductionRequest.getString("inputPath")).thenReturn("/calvalus/auxiliary/urban-footprint/ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.tif");
        when(mockProductionRequest.getString("regionWKT")).thenReturn("POLYGON((100 -10,100 0,110 0,110 -10,100 -10))");
        when(mockProductionRequest.getString("processorBundleVersion")).thenReturn("1.0");
        when(mockProductionRequest.getString("productionType")).thenReturn("L2Plus");
        when(mockProductionRequest.getString("outputFormat")).thenReturn("NetCDF4");
        when(mockProductionRequest.getStagingDirectory(anyString())).thenReturn("hans/20160317_10000000");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);

        when(mockProduction.getName()).thenReturn("TEP Subset test");
        when(mockProduction.getId()).thenReturn("20160429140605_L2Plus_16bd8f26b258fc");

        WorkflowItem mockWorkflowItem = mock(WorkflowItem.class);
        when(mockWorkflowItem.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflowItem);
    }

    private void configureMockServerContext() {
        mockServerContext = mock(WpsServerContext.class);
        when(mockServerContext.getHostAddress()).thenReturn("http://www.brockmann-consult.de");
        when(mockServerContext.getPort()).thenReturn(80);
        when(mockServerContext.getRequestUrl()).thenReturn("http://www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetStatus&JobId=20160429140605_L2Plus_16bd8f26b258fc");
    }
}