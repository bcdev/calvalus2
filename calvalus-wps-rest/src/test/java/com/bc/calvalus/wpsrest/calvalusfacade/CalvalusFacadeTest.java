package com.bc.calvalus.wpsrest.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

/**
 * @author hans
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusFacade.class, CalvalusProduction.class,
            CalvalusProductionService.class, CalvalusProductionService.class
})
public class CalvalusFacadeTest {

    private ServletRequestWrapper mockServletRequestWrapper;
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
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        mockCalvalusProduction = mock(CalvalusProduction.class);
        mockCalvalusStaging = mock(CalvalusStaging.class);
        mockCalvalusProcessorExtractor = mock(CalvalusProcessorExtractor.class);

        when(mockServletRequestWrapper.getUserName()).thenReturn("mockUserName");

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

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
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

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
        calvalusFacade.orderProductionSynchronous(mockProductionRequest);

        verify(mockCalvalusProduction).orderProductionSynchronous(any(ProductionService.class), requestArgumentCaptor.capture());

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockProductionRequest));
    }

    @Test
    public void testGetProductResultUrls() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
        calvalusFacade.getProductResultUrls(mockProduction);

        verify(mockCalvalusStaging).getProductResultUrls(anyMapOf(String.class, String.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testStageProduction() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
        calvalusFacade.stageProduction(mockProduction);

        verify(mockCalvalusStaging).stageProduction(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testObserveStagingStatus() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
        calvalusFacade.observeStagingStatus(mockProduction);

        verify(mockCalvalusStaging).observeStagingStatus(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testGetProcessors() throws Exception {
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);

        ArgumentCaptor<ProductionService> productionServiceCaptor = ArgumentCaptor.forClass(ProductionService.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
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

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
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
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);

        calvalusFacade = new CalvalusFacade(mockServletRequestWrapper);
        calvalusFacade.getProductSets();

        verify(mockProductionService).getProductSets(anyString(), anyString());
    }

    private void configureProductionServiceMocking() throws IOException, ProductionException {
        mockProductionService = mock(ProductionService.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);
    }
}