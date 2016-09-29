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
import com.bc.calvalus.wps.cmd.LdapHelper;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
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
            CalvalusProductionService.class, CalvalusProductionService.class,
            LdapHelper.class
})
public class CalvalusFacadeTest {

    private static final String MOCK_USER_NAME = "mockUserName";

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

        when(mockRequestContext.getUserName()).thenReturn(MOCK_USER_NAME);

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
        when(mockProductionService.getProduction("job-00")).thenReturn(mockProduction);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProductResultUrls("job-00");

        verify(mockCalvalusStaging).getProductResultUrls(anyMapOf(String.class, String.class), productionCaptor.capture());

        assertThat(productionCaptor.getValue(), equalTo(mockProduction));
    }

    @Test
    public void testStageProduction() throws Exception {
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<String> jobidCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.stageProduction("job-00");

        verify(mockCalvalusStaging).stageProduction(any(ProductionService.class), jobidCaptor.capture());

        assertThat(jobidCaptor.getValue(), equalTo("job-00"));
    }

    @Test
    public void testObserveStagingStatus() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        when(mockProductionService.getProduction("job-00")).thenReturn(mockProduction);
        ArgumentCaptor<Production> productionCaptor = ArgumentCaptor.forClass(Production.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.observeStagingStatus("job-00");

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
        ProcessorNameConverter mockParser = mock(ProcessorNameConverter.class);
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);
        ArgumentCaptor<ProcessorNameConverter> parserCaptor = ArgumentCaptor.forClass(ProcessorNameConverter.class);
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
    public void canIgnoreExceptionWhenGetNullProductSets() throws Exception {
        PowerMockito.mockStatic(CalvalusProductionService.class);
        ProductionService mockProductionService = mock(ProductionService.class);
        ProductSet[] mockProductSets = new ProductSet[]{};
        when(mockProductionService.getProductSets("mockUserName", "")).thenReturn(mockProductSets);
        when(mockProductionService.getProductSets("mockUserName", "user=mockUserName")).thenThrow(new ProductionException("null productsets"));
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
    public void canResolveAndRegisterRemoteUserName() throws Exception {
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(MOCK_USER_NAME);
        LdapHelper mockLdapHelper = mock(LdapHelper.class);
        when(mockLdapHelper.isRegistered(anyString())).thenReturn(false);
        PowerMockito.whenNew(LdapHelper.class).withNoArguments().thenReturn(mockLdapHelper);
        ArgumentCaptor<String> ldapUser = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        verify(mockLdapHelper, times(1)).register(ldapUser.capture());

        assertThat(ldapUser.getValue(), equalTo("tep_mockUserName"));
        assertThat(calvalusFacade.getUserName(), equalTo("tep_mockUserName"));
    }

    @Test
    public void canResolveRemoteUserName() throws Exception {
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(MOCK_USER_NAME);
        LdapHelper mockLdapHelper = mock(LdapHelper.class);
        when(mockLdapHelper.isRegistered(anyString())).thenReturn(true);
        PowerMockito.whenNew(LdapHelper.class).withNoArguments().thenReturn(mockLdapHelper);

        calvalusFacade = new CalvalusFacade(mockRequestContext);

        assertThat(calvalusFacade.getUserName(), equalTo("tep_mockUserName"));
    }

    @Test
    public void canGetUserNameWhenNoRemoteUser() throws Exception {
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(null);
        when(mockRequestContext.getUserName()).thenReturn(MOCK_USER_NAME);

        calvalusFacade = new CalvalusFacade(mockRequestContext);

        assertThat(calvalusFacade.getUserName(), equalTo("mockUserName"));

    }

    private void configureProductionServiceMocking() throws IOException, ProductionException {
        mockProductionService = mock(ProductionService.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);
    }

}