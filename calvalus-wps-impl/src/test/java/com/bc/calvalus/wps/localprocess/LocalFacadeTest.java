package com.bc.calvalus.wps.localprocess;


import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalFacade.class, LocalStaging.class, LocalProduction.class})
public class LocalFacadeTest {

    private static final String DUMMY_USER = "dummyUser";
    private static final String DUMMY_HOST_NAME = "www.dummy-host.de";
    private static final int DUMMY_PORT_NUMBER = 80;
    private static final String DUMMY_JOB_ID = "job-00";

    private WpsRequestContext mockRequestContext;
    private LocalProduction mockLocalProduction;
    private LocalStaging mockLocalStaging;
    private LocalProcessorExtractor mockProcessorExtractor;

    private LocalFacade localFacade;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");

        mockRequestContext = mock(WpsRequestContext.class);
        WpsServerContext mockServerContext = mock(WpsServerContext.class);
        mockLocalProduction = mock(LocalProduction.class);
        mockLocalStaging = mock(LocalStaging.class);
        mockProcessorExtractor = mock(LocalProcessorExtractor.class);

        when(mockServerContext.getHostAddress()).thenReturn(DUMMY_HOST_NAME);
        when(mockServerContext.getPort()).thenReturn(DUMMY_PORT_NUMBER);
        when(mockRequestContext.getUserName()).thenReturn(DUMMY_USER);
        when(mockRequestContext.getServerContext()).thenReturn(mockServerContext);
    }

    @Test
    public void canOrderProductionAsynchronous() throws Exception {
        Execute mockExecute = mock(Execute.class);
        PowerMockito.whenNew(LocalProduction.class).withNoArguments().thenReturn(mockLocalProduction);

        ArgumentCaptor<Execute> executeCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<WpsRequestContext> requestContextCaptor = ArgumentCaptor.forClass(WpsRequestContext.class);
        ArgumentCaptor<LocalFacade> localFacadeCaptor = ArgumentCaptor.forClass(LocalFacade.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.orderProductionAsynchronous(mockExecute);

        verify(mockLocalProduction).orderProductionAsynchronous(executeCaptor.capture(),
                                                                userNameCaptor.capture(),
                                                                requestContextCaptor.capture(),
                                                                localFacadeCaptor.capture());

        assertThat(executeCaptor.getValue(), equalTo(mockExecute));
        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
        assertThat(requestContextCaptor.getValue(), equalTo(mockRequestContext));
        assertThat(localFacadeCaptor.getValue(), equalTo(localFacade));
    }

    @Test
    public void canOrderProductionSynchronous() throws Exception {
        Execute mockExecute = mock(Execute.class);
        PowerMockito.whenNew(LocalProduction.class).withNoArguments().thenReturn(mockLocalProduction);

        ArgumentCaptor<Execute> executeCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<WpsRequestContext> requestContextCaptor = ArgumentCaptor.forClass(WpsRequestContext.class);
        ArgumentCaptor<LocalFacade> localFacadeCaptor = ArgumentCaptor.forClass(LocalFacade.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.orderProductionSynchronous(mockExecute);

        verify(mockLocalProduction).orderProductionSynchronous(executeCaptor.capture(),
                                                               userNameCaptor.capture(),
                                                               requestContextCaptor.capture(),
                                                               localFacadeCaptor.capture());

        assertThat(executeCaptor.getValue(), equalTo(mockExecute));
        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
        assertThat(requestContextCaptor.getValue(), equalTo(mockRequestContext));
        assertThat(localFacadeCaptor.getValue(), equalTo(localFacade));
    }

    @Test
    public void canGetProductResultUrls() throws Exception {
        PowerMockito.whenNew(LocalStaging.class).withNoArguments().thenReturn(mockLocalStaging);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> hostNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> portNumberCaptor = ArgumentCaptor.forClass(Integer.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.getProductResultUrls(DUMMY_JOB_ID);

        verify(mockLocalStaging).getProductUrls(jobIdCaptor.capture(),
                                                userNameCaptor.capture(),
                                                hostNameCaptor.capture(),
                                                portNumberCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("job-00"));
        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
        assertThat(hostNameCaptor.getValue(), equalTo("www.dummy-host.de"));
        assertThat(portNumberCaptor.getValue(), equalTo(80));
    }

    @Test
    public void canGenerateProductMetadata() throws Exception {
        PowerMockito.whenNew(LocalStaging.class).withNoArguments().thenReturn(mockLocalStaging);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> hostNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> portNumberCaptor = ArgumentCaptor.forClass(Integer.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.generateProductMetadata(DUMMY_JOB_ID);

        verify(mockLocalStaging).generateProductMetadata(jobIdCaptor.capture(),
                                                         userNameCaptor.capture(),
                                                         hostNameCaptor.capture(),
                                                         portNumberCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("job-00"));
        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
        assertThat(hostNameCaptor.getValue(), equalTo("www.dummy-host.de"));
        assertThat(portNumberCaptor.getValue(), equalTo(80));
    }

    @Test
    public void canGetProcessor() throws Exception {
        PowerMockito.whenNew(LocalProcessorExtractor.class).withNoArguments().thenReturn(mockProcessorExtractor);
        ProcessorNameConverter mockConverter = mock(ProcessorNameConverter.class);

        ArgumentCaptor<ProcessorNameConverter> converterCaptor = ArgumentCaptor.forClass(ProcessorNameConverter.class);
        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.getProcessor(mockConverter);

        verify(mockProcessorExtractor).getProcessor(converterCaptor.capture(), userNameCaptor.capture());

        assertThat(converterCaptor.getValue(), equalTo(mockConverter));
        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
    }

    @Test
    public void canGetProcessors() throws Exception {
        PowerMockito.whenNew(LocalProcessorExtractor.class).withNoArguments().thenReturn(mockProcessorExtractor);

        ArgumentCaptor<String> userNameCaptor = ArgumentCaptor.forClass(String.class);

        localFacade = new LocalFacade(mockRequestContext);
        localFacade.getProcessors();

        verify(mockProcessorExtractor).getProcessors(userNameCaptor.capture());

        assertThat(userNameCaptor.getValue(), equalTo("dummyUser"));
    }
}