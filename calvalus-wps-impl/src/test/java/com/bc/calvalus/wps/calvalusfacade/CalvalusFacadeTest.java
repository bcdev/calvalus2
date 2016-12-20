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
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.CodeType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusFacade.class, CalvalusProduction.class,
            CalvalusProductionService.class, LdapHelper.class, ExecuteRequestExtractor.class,
            ProcessorNameConverter.class, CalvalusProcessorExtractor.class, CalvalusDataInputs.class,
            ProductionRequest.class
})
public class CalvalusFacadeTest {

    private static final String MOCK_USER_NAME = "mockUserName";
    private static final String MOCK_PROCESS_ID = "process-00";

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
        Execute mockExecuteRequest = mock(Execute.class);
        CalvalusProcessor mockProcessor = mock(CalvalusProcessor.class);
        CodeType mockIdentifier = new CodeType();
        mockIdentifier.setValue(MOCK_PROCESS_ID);
        when(mockExecuteRequest.getIdentifier()).thenReturn(mockIdentifier);
        ExecuteRequestExtractor mockRequestExtractor = mock(ExecuteRequestExtractor.class);
        ProcessorNameConverter mockNameConverter = mock(ProcessorNameConverter.class);
        CalvalusDataInputs mockCalvalusDataInput = mock(CalvalusDataInputs.class);
        Map<String, String> mockParameterMap = new HashMap<>();
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockCalvalusProcessorExtractor.getProcessor(any(ProcessorNameConverter.class), anyString()))
                    .thenReturn(mockProcessor);
        when(mockCalvalusDataInput.getValue("productionType")).thenReturn("L2");
        when(mockCalvalusDataInput.getInputMapFormatted()).thenReturn(mockParameterMap);
        whenNew(CalvalusProduction.class).withNoArguments().thenReturn(mockCalvalusProduction);
        whenNew(ExecuteRequestExtractor.class).withArguments(Execute.class).thenReturn(mockRequestExtractor);
        whenNew(ProcessorNameConverter.class).withArguments(anyString()).thenReturn(mockNameConverter);
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);
        whenNew(CalvalusDataInputs.class).withAnyArguments().thenReturn(mockCalvalusDataInput);
        whenNew(ProductionRequest.class).withAnyArguments().thenReturn(mockProductionRequest);
        ArgumentCaptor<Execute> requestArgumentCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.orderProductionAsynchronous(mockExecuteRequest);

        verify(mockCalvalusProduction).orderProductionAsynchronous(requestArgumentCaptor.capture(), userNameCaptor.capture(), any(CalvalusFacade.class));

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockExecuteRequest));
        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testOrderProductionSynchronous() throws Exception {
        Execute mockExecuteRequest = mock(Execute.class);
        CalvalusProcessor mockProcessor = mock(CalvalusProcessor.class);
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        CodeType mockIdentifier = new CodeType();
        mockIdentifier.setValue(MOCK_PROCESS_ID);
        when(mockExecuteRequest.getIdentifier()).thenReturn(mockIdentifier);
        ExecuteRequestExtractor mockRequestExtractor = mock(ExecuteRequestExtractor.class);
        ProcessorNameConverter mockNameConverter = mock(ProcessorNameConverter.class);
        CalvalusDataInputs mockCalvalusDataInput = mock(CalvalusDataInputs.class);
        Map<String, String> mockParameterMap = new HashMap<>();
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockCalvalusProcessorExtractor.getProcessor(any(ProcessorNameConverter.class), anyString()))
                    .thenReturn(mockProcessor);
        when(mockCalvalusDataInput.getValue("productionType")).thenReturn("L2");
        when(mockCalvalusDataInput.getInputMapFormatted()).thenReturn(mockParameterMap);
        when(mockStatus.getJobId()).thenReturn(MOCK_PROCESS_ID);
        when(mockCalvalusProduction.orderProductionSynchronous(any(Execute.class), anyString(), any(CalvalusFacade.class))).thenReturn(mockStatus);
        whenNew(ExecuteRequestExtractor.class).withArguments(Execute.class).thenReturn(mockRequestExtractor);
        whenNew(ProcessorNameConverter.class).withArguments(anyString()).thenReturn(mockNameConverter);
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);
        whenNew(CalvalusDataInputs.class).withAnyArguments().thenReturn(mockCalvalusDataInput);
        whenNew(ProductionRequest.class).withAnyArguments().thenReturn(mockProductionRequest);
        whenNew(CalvalusProduction.class).withNoArguments().thenReturn(mockCalvalusProduction);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<Execute> requestArgumentCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> userNameArgumentCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.orderProductionSynchronous(mockExecuteRequest);

        verify(mockCalvalusProduction).orderProductionSynchronous(requestArgumentCaptor.capture(), userNameArgumentCaptor.capture(), any(CalvalusFacade.class));

        assertThat(requestArgumentCaptor.getValue(), equalTo(mockExecuteRequest));
    }

    @Test
    public void testGetProductResultUrls() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        when(mockProductionService.getProduction("job-00")).thenReturn(mockProduction);
        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProductResultUrls("job-00");

        verify(mockCalvalusStaging).getProductResultUrls(jobIdCaptor.capture(), anyMapOf(String.class, String.class));

        assertThat(jobIdCaptor.getValue(), equalTo("job-00"));
    }

    @Test
    public void testStageProduction() throws Exception {
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        ArgumentCaptor<String> jobidCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.stageProduction("job-00");

        verify(mockCalvalusStaging).stageProduction(jobidCaptor.capture());

        assertThat(jobidCaptor.getValue(), equalTo("job-00"));
    }

    @Test
    public void testObserveStagingStatus() throws Exception {
        Production mockProduction = mock(Production.class);
        whenNew(CalvalusStaging.class).withArguments(any(WpsServerContext.class)).thenReturn(mockCalvalusStaging);
        when(mockProductionService.getProduction("job-00")).thenReturn(mockProduction);
        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.observeStagingStatus("job-00");

        verify(mockCalvalusStaging).observeStagingStatus(jobIdCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("job-00"));
    }

    @Test
    public void testGetProcessors() throws Exception {
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);

        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProcessors();

        verify(mockCalvalusProcessorExtractor).getProcessors(userNameCaptor.capture());

        assertThat(userNameCaptor.getValue(), equalTo("mockUserName"));
    }

    @Test
    public void testGetProcessor() throws Exception {
        ProcessorNameConverter mockParser = mock(ProcessorNameConverter.class);
        whenNew(CalvalusProcessorExtractor.class).withNoArguments().thenReturn(mockCalvalusProcessorExtractor);
        ArgumentCaptor<ProcessorNameConverter> parserCaptor = ArgumentCaptor.forClass(ProcessorNameConverter.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        calvalusFacade.getProcessor(mockParser);

        verify(mockCalvalusProcessorExtractor).getProcessor(parserCaptor.capture(), userNameCaptor.capture());

        assertThat(parserCaptor.getValue(), equalTo(mockParser));
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
    public void canResolveRemoteUserNameFromCache() throws Exception {
        PowerMockito.mockStatic(CalvalusProductionService.class);
        Set<String> dummyRemoteUserSet = new HashSet<>();
        dummyRemoteUserSet.add("tep_mockUserName");
        dummyRemoteUserSet.add("tep_mockUserName2");
        PowerMockito.when(CalvalusProductionService.getRemoteUserSet()).thenReturn(dummyRemoteUserSet);
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(MOCK_USER_NAME);
        LdapHelper mockLdapHelper = mock(LdapHelper.class);
        when(mockLdapHelper.isRegistered(anyString())).thenReturn(false);
        PowerMockito.whenNew(LdapHelper.class).withNoArguments().thenReturn(mockLdapHelper);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        verify(mockLdapHelper, times(0)).register(anyString());

        assertThat(calvalusFacade.getRemoteUserName(), equalTo("tep_mockUserName"));
    }

    @Test
    public void canResolveAndRegisterRemoteUserName() throws Exception {
        PowerMockito.mockStatic(CalvalusProductionService.class);
        Set<String> dummyRemoteUserSet = new HashSet<>();
        PowerMockito.when(CalvalusProductionService.getRemoteUserSet()).thenReturn(dummyRemoteUserSet);
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(MOCK_USER_NAME);
        LdapHelper mockLdapHelper = mock(LdapHelper.class);
        when(mockLdapHelper.isRegistered(anyString())).thenReturn(false);
        PowerMockito.whenNew(LdapHelper.class).withNoArguments().thenReturn(mockLdapHelper);
        ArgumentCaptor<String> ldapUser = ArgumentCaptor.forClass(String.class);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        verify(mockLdapHelper, times(1)).register(ldapUser.capture());

        assertThat(ldapUser.getValue(), equalTo("tep_mockUserName"));
        assertThat(calvalusFacade.getRemoteUserName(), equalTo("tep_mockUserName"));
        assertThat(dummyRemoteUserSet.size(), equalTo(1));
        assertThat(dummyRemoteUserSet.iterator().next(), equalTo("tep_mockUserName"));
    }

    @Test
    public void canResolveRemoteUserName() throws Exception {
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(MOCK_USER_NAME);
        LdapHelper mockLdapHelper = mock(LdapHelper.class);
        when(mockLdapHelper.isRegistered(anyString())).thenReturn(true);
        PowerMockito.whenNew(LdapHelper.class).withNoArguments().thenReturn(mockLdapHelper);

        calvalusFacade = new CalvalusFacade(mockRequestContext);
        verify(mockLdapHelper, times(0)).register(anyString());

        assertThat(calvalusFacade.getRemoteUserName(), equalTo("tep_mockUserName"));
    }

    @Test
    public void canGetUserNameWhenNoRemoteUser() throws Exception {
        when(mockRequestContext.getHeaderField("remote_user")).thenReturn(null);
        when(mockRequestContext.getUserName()).thenReturn(MOCK_USER_NAME);

        calvalusFacade = new CalvalusFacade(mockRequestContext);

        assertThat(calvalusFacade.getRemoteUserName(), equalTo("mockUserName"));

    }

    private void configureProductionServiceMocking() throws IOException, ProductionException {
        mockProductionService = mock(ProductionService.class);
        when(mockProductionService.getProductSets(anyString(), anyString())).thenReturn(new ProductSet[0]);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        PowerMockito.when(CalvalusProductionService.getProductionServiceSingleton()).thenReturn(mockProductionService);
    }

}