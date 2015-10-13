package com.bc.calvalus.wpsrest.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by hans on 15/09/2015.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusHelper.class, CalvalusProduction.class})
public class CalvalusHelperTest {

    private ServletRequestWrapper mockServletRequestWrapper;
    private CalvalusProduction mockCalvalusProduction;
    private CalvalusStaging mockCalvalusStaging;
    private CalvalusProcessorExtractor mockCalvalusProcessorExtractor;

    /**
     * Class under test.
     */
    private CalvalusHelper calvalusHelper;

    @Before
    public void setUp() throws Exception {
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        mockCalvalusProduction = mock(CalvalusProduction.class);
        mockCalvalusStaging = mock(CalvalusStaging.class);
        mockCalvalusProcessorExtractor = mock(CalvalusProcessorExtractor.class);

        when(mockServletRequestWrapper.getUserName()).thenReturn("mockUserName");
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

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.orderProductionAsynchronous(mockProductionRequest);

        verify(mockCalvalusProduction).orderProductionAsynchronous(any(ProductionService.class), requestArgumentCaptor.capture(), userNameCaptor.capture());

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockProductionRequest));
        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testOrderProductionSynchronous() throws Exception {
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        whenNew(CalvalusProduction.class).withNoArguments().thenReturn(mockCalvalusProduction);
        ArgumentCaptor<ProductionRequest> requestArgumentCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.orderProductionSynchronous(mockProductionRequest);

        verify(mockCalvalusProduction).orderProductionSynchronous(any(ProductionService.class), requestArgumentCaptor.capture());

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockProductionRequest));
    }

    @Test
    public void testGetProductResultUrls() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.getProductResultUrls(mockProduction);

        verify(mockCalvalusStaging).getProductResultUrls(anyMapOf(String.class, String.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testStageProduction() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.stageProduction(mockProduction);

        verify(mockCalvalusStaging).stageProduction(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testObserveStagingStatus() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.observeStagingStatus(mockProduction);

        verify(mockCalvalusStaging).observeStagingStatus(any(ProductionService.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testGetProcessors() throws Exception {
        whenNew(CalvalusProcessorExtractor.class).withArguments(any(ProductionService.class), anyString()).thenReturn(mockCalvalusProcessorExtractor);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.getProcessors();

        verify(mockCalvalusProcessorExtractor).getProcessors();
    }

    @Test
    public void testGetProcessor() throws Exception {
        ProcessorNameParser mockParser = mock(ProcessorNameParser.class);
        whenNew(CalvalusProcessorExtractor.class).withArguments(any(ProductionService.class), anyString()).thenReturn(mockCalvalusProcessorExtractor);
        ArgumentCaptor<ProcessorNameParser> parserCaptor = ArgumentCaptor.forClass(ProcessorNameParser.class);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.getProcessor(mockParser);

        verify(mockCalvalusProcessorExtractor).getProcessor(parserCaptor.capture());

        assertThat(parserCaptor.getValue(), equalTo(mockParser));
    }

    @Test
    public void testGetProductSets() throws Exception {
        whenNew(CalvalusProcessorExtractor.class).withArguments(any(ProductionService.class), anyString()).thenReturn(mockCalvalusProcessorExtractor);

        calvalusHelper = new CalvalusHelper(mockServletRequestWrapper);
        calvalusHelper.getProductSets();

        verify(mockCalvalusProcessorExtractor).getProductSets();
    }
}