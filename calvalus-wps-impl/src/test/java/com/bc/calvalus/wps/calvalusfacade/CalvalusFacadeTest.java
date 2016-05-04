package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import org.apache.commons.collections.map.HashedMap;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusFacade.class, CalvalusProduction.class,
            CalvalusProductionService.class, CalvalusProductionService.class
})
public class CalvalusFacadeTest {

    private WpsRequestContext mockRequestContext;
    private CalvalusProduction mockCalvalusProduction;
    private CalvalusStaging mockCalvalusStaging;
    private CalvalusProcessorExtractor mockCalvalusProcessorExtractor;
    private ProductionService mockProductionService;

    /**
     * Class under test.
     */
    private CalvalusFacade calvalusFacade;

    @Before
    public void setUp() throws Exception {
        mockRequestContext = mock(WpsRequestContext.class);
        mockCalvalusProduction = mock(CalvalusProduction.class);
        mockCalvalusStaging = mock(CalvalusStaging.class);
        mockCalvalusProcessorExtractor = mock(CalvalusProcessorExtractor.class);

        when(mockRequestContext.getUserName()).thenReturn("mockUserName");

        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        configureProductionServiceMocking();
    }

    @Test
    public void testGetProductionService() throws Exception {

    }

    @Test
    public void testOrderProductionAsynchronous() throws Exception {
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        whenNew(CalvalusProduction.class).withNoArguments().thenReturn(mockCalvalusProduction);
        ArgumentCaptor<ProductionRequest> requestArgumentCaptor = ArgumentCaptor.forClass(ProductionRequest.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.orderProductionAsynchronous(mockProductionRequest);

        verify(mockCalvalusProduction).orderProductionAsynchronous(any(ProductionService.class), requestArgumentCaptor.capture(), userNameCaptor.capture());

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockProductionRequest));
        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testOrderProductionSynchronous() throws Exception {
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        whenNew(CalvalusProduction.class).withNoArguments().thenReturn(mockCalvalusProduction);
        ArgumentCaptor<ProductionRequest> requestArgumentCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.orderProductionSynchronous(mockProductionRequest);

        verify(mockCalvalusProduction).orderProductionSynchronous(any(ProductionService.class), requestArgumentCaptor.capture());

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockProductionRequest));
    }

    @Test
    public void testGetProductResultUrls() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProductResultUrls(mockProduction);

        verify(mockCalvalusStaging).getProductResultUrls(anyMapOf(String.class, String.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testStageProduction() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.stageProduction(mockProduction);

        verify(mockCalvalusStaging).stageProduction(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testObserveStagingStatus() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.observeStagingStatus(mockProduction);

        verify(mockCalvalusStaging).observeStagingStatus(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testGetProcessors() throws Exception {
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);

        ArgumentCaptor<ProductionService> productionServiceCaptor = ArgumentCaptor.forClass(ProductionService.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProcessors();

        verify(mockCalvalusProcessorExtractor).getProcessors(productionServiceCaptor.capture(), userNameCaptor.capture());

        assertThat(productionServiceCaptor.getValue(), equalTo(mockProductionService));
        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testGetProcessor() throws Exception {
        ProcessorNameParser mockParser = mock(ProcessorNameParser.class);
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);
        ArgumentCaptor<ProcessorNameParser> parserCaptor = ArgumentCaptor.forClass(ProcessorNameParser.class);
        ArgumentCaptor<ProductionService> productionServiceCaptor = ArgumentCaptor.forClass(ProductionService.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProcessor(mockParser);

        verify(mockCalvalusProcessorExtractor).getProcessor(parserCaptor.capture(), productionServiceCaptor.capture(), userNameCaptor.capture());

        assertThat(parserCaptor.getValue(), equalTo(mockParser));
        assertThat(productionServiceCaptor.getValue(), equalTo(mockProductionService));
        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testGetProductSets() throws Exception {
        PowerMockito.mockStatic(CalvalusProductionService.class);
        ProductionService mockProductionService = mock(ProductionService.class);
        ProductSet[] mockProductSets = new ProductSet[]{};
        when(mockProductionService.getProductSets(anyString(), anyString())).thenReturn(mockProductSets);
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);
        ArgumentCaptor<String> arg1 = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> arg2 = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProductSets();

        verify(mockProductionService, times(2)).getProductSets(arg1.capture(), arg2.capture());

        assertThat((arg1.getAllValues().get(0)), equalTo("mockUserName"));
        assertThat((arg2.getAllValues().get(0)), equalTo(""));

        assertThat((arg1.getAllValues().get(1)), equalTo("mockUserName"));
        assertThat((arg2.getAllValues().get(1)), equalTo("user=mockUserName"));
    }

    @Test
    public void testVelocity() throws Exception {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();

        ArrayList<Map> list = new ArrayList();
        Map<String, String> map = new HashMap<>();

        map.put("name", "Cow");
        map.put("price", "$100.00");
        list.add(map);

        map = new HashMap<>();
        map.put("name", "Eagle");
        map.put("price", "$59.99");
        list.add(map);

        map = new HashMap<>();
        map.put("name", "Shark");
        map.put("price", "$3.99");
        list.add(map);

        VelocityContext context = new VelocityContext();
        context.put("test", "attribute");
        context.put("petList", list);

        Template t = ve.getTemplate("test-velocity.vm");

        StringWriter writer = new StringWriter();

        t.merge(context, writer);

        System.out.println(writer.toString());
    }

    @Test
    public void testMetadataVelocity() throws Exception {
        VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        ve.init();

        VelocityContext context = new VelocityContext();
        Template template = ve.getTemplate("test-velocity2.vm");
        StringWriter writer = new StringWriter();

        context.put("jobUrl", "http://www.brockmann-consult.de/bc-wps/wps/calvalus?Service=WPS&Request=GetStatus&JobId=20160317105702_L3_8ae4f737a2b6");
        context.put("jobFinishTime", "2016-03-17T14:00:00.000000Z");
        context.put("productOutputDir", "Jakarta GUF test/hans/20160317_10000000");
        context.put("productionName", "Jakarta GUF test");
        context.put("processName", "Subset");
        context.put("inputDatasetName", "Urban Footprint Global (Urban TEP)");
        context.put("stagingDir", "http://www.brockmann-consult.de/bc-wps/staging/hans");
        context.put("regionWkt", "100 -10 100 0 110 0 110 -10 100 -10");
        context.put("startDate", "2008-01-01T00:00:00Z");
        context.put("stopDate", "2012-12-31T23:59:59Z");
        context.put("collectionUrl", "http://www.brockmann-consult.de/bc-wps/staging/hans/20160317_10000000");
        context.put("productOutputPath", "Jakarta GUF test/hans/20160317_10000000");
        context.put("processorVersion", "3.0");
        context.put("productionType", "L2");
        context.put("outputFormat", "NetCDF-4");

        List<Map> productList = new ArrayList<>();
        Map<String, String> product1 = new HashMap<>();
        product1.put("productUrl","http://www.brockmann-consult.de/bc-wps/staging/hans/20160317092654_L2Plus_85f7236d9c82/L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.nc");
        product1.put("productFileName","L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.nc");
        product1.put("productFileFormat","NetCDF-4");
        product1.put("productFileSize","123000");
        product1.put("productQuickLookUrl","http://www.brockmann-consult.de/bc-wps/staging/hans/20160317092654_L2Plus_85f7236d9c82/L2_of_ESACCI-LC-L4-LCCS-Map-300m-P5Y-20100101-v1.6.1_urban_bit_lzw.png");
        productList.add(product1);

        context.put("productList", productList);

        template.merge(context, writer);

        System.out.println(writer);
    }

    private void configureProductionServiceMocking() throws IOException, ProductionException {
        mockProductionService = mock(ProductionService.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);
    }

}