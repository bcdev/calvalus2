package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusStaging.class, CalvalusProductionService.class, CalvalusLogger.class, File.class})
public class CalvalusStagingTest {

    private static final String DUMMY_PATH = "dummyPath";
    private static final String DUMMY_JOB_ID = "jobId";
    private static final String DUMMY_USER_NAME = "dummyUser";

    private WpsServerContext mockServerContext;
    private Production mockProduction;
    private ProductionService mockProductionService;

    /**
     * Class under test.
     */
    private CalvalusStaging calvalusStaging;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockServerContext = mock(WpsServerContext.class);
        mockProduction = mock(Production.class);


        when(mockServerContext.getHostAddress()).thenReturn("calvalustomcat-test");
        when(mockServerContext.getPort()).thenReturn(8080);

        PowerMockito.mockStatic(Thread.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);

    }

    @Test
    public void canStageProduction() throws Exception {
        mockProductionService = mock(ProductionService.class);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);

        ArgumentCaptor<String> productId = ArgumentCaptor.forClass(String.class);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.stageProduction("productId");

        verify(mockProductionService).stageProductions(productId.capture());
        assertThat(productId.getValue(), equalTo("productId"));
    }

    @Test(expected = WpsStagingException.class)
    public void canCatchExceptionWhenStageProduction() throws Exception {
        mockProductionService = mock(ProductionService.class);
        doThrow(new ProductionException("error when staging")).when(mockProductionService).stageProductions(anyString());
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);

        ArgumentCaptor<String> productId = ArgumentCaptor.forClass(String.class);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.stageProduction("productId");

        verify(mockProductionService).stageProductions(productId.capture());
        assertThat(productId.getValue(), equalTo("productId"));
    }

    @Test
    public void canGetProductResultUrls() throws Exception {
        mockProductionService = mock(ProductionService.class);
        when(mockProduction.getName()).thenReturn(DUMMY_JOB_ID);
        when(mockProduction.getStagingPath()).thenReturn(DUMMY_PATH);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();
        File mockFile = mock(File.class);
        File[] productResultFiles = getProductResultFiles();
        when(mockFile.listFiles()).thenReturn(productResultFiles);
        PowerMockito.whenNew(File.class).withArguments(anyString()).thenReturn(mockFile);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        List<String> productResultUrls = calvalusStaging.getProductResultUrls("jobId", mockCalvalusDefaultConfig);

        assertThat(productResultUrls.size(), equalTo(2));
        assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/dummyPath/product1.nc"));
        assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/dummyPath/product2.nc"));
        // not clear how to trigger generation of the metadata file in this mocked setup
        //assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/dummyPath/jobId-metadata"));
    }

    @Test
    public void canGetEmptyProductResultUrlsWhenNoProducts() throws Exception {
        mockProductionService = mock(ProductionService.class);
        when(mockProduction.getName()).thenReturn(DUMMY_JOB_ID);
        when(mockProduction.getStagingPath()).thenReturn(DUMMY_PATH);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();
        File mockFile = mock(File.class);
        when(mockFile.listFiles()).thenReturn(new File[0]);
        PowerMockito.whenNew(File.class).withArguments(anyString()).thenReturn(mockFile);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        List<String> productResultUrls = calvalusStaging.getProductResultUrls("jobId", mockCalvalusDefaultConfig);

        assertThat(productResultUrls.size(), equalTo(0));
        // not clear how to trigger generation of the metadata file in this mocked setup
        //assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/dummyPath/jobId-metadata"));
    }

    @Test
    public void canGetEmptyProductResultUrlsWhenDirectoryNotExist() throws Exception {
        mockProductionService = mock(ProductionService.class);
        when(mockProduction.getName()).thenReturn(DUMMY_JOB_ID);
        when(mockProduction.getStagingPath()).thenReturn(DUMMY_PATH);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();
        File mockFile = mock(File.class);
        when(mockFile.listFiles()).thenReturn(null);
        PowerMockito.whenNew(File.class).withArguments(anyString()).thenReturn(mockFile);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        List<String> productResultUrls = calvalusStaging.getProductResultUrls("jobId", mockCalvalusDefaultConfig);

        assertThat(productResultUrls.size(), equalTo(0));
    }

    @Test(expected = WpsResultProductException.class)
    public void canCatchExceptionWhenGetResultUrls() throws Exception {
        mockProductionService = mock(ProductionService.class);
        when(mockProductionService.getProduction(anyString())).thenThrow(new ProductionException("error in getting production"));
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.getProductResultUrls("jobId", mockCalvalusDefaultConfig);
    }

    @Ignore // TODO : issue with the mocking of Logger. It runs when the test is run individually, but not when all the test cases are run
    @Test
    public void canObserveStagingStatusStillRunning() throws Exception {
        PowerMockito.mockStatic(CalvalusLogger.class);
        PowerMockito.mockStatic(Thread.class);
        PowerMockito.mockStatic(CalvalusProductionService.class);
        Logger mockLogger = mock(Logger.class);
        PowerMockito.when(CalvalusLogger.getLogger()).thenReturn(mockLogger);
        mockProductionService = mock(ProductionService.class);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        ProcessStatus mockProcessStatus = getProcessStatusStillRunning();
        when(mockProductionRequest.getUserName()).thenReturn(DUMMY_USER_NAME);
        when(mockProduction.getName()).thenReturn(DUMMY_JOB_ID);
        when(mockProduction.getStagingPath()).thenReturn(DUMMY_PATH);
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);

        ArgumentCaptor<String> userCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.observeStagingStatus("jobId");

        verify(mockProductionService, times(2)).updateStatuses(userCaptor.capture());
        verify(mockLogger, times(3)).info(statusCaptor.capture());

        assertThat(userCaptor.getValue(), equalTo("dummyUser"));
        assertThat(statusCaptor.getAllValues().get(0), equalTo("Staging status: state=RUNNING, progress=0.8, message='Still running'"));
        assertThat(statusCaptor.getAllValues().get(1), equalTo("Staging status: state=RUNNING, progress=0.9, message='Still running'"));
        assertThat(statusCaptor.getAllValues().get(2), equalTo("Staging completed."));
    }

    @Ignore // TODO : issue with the mocking of Logger. It runs when the test is run individually, but not when all the test cases are run
    @Test
    public void canObserveStagingStatusAlreadyDone() throws Exception {
        mockProductionService = mock(ProductionService.class);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        ProcessStatus mockProcessStatus = getProcessStatusAlreadyDone();
        when(mockProductionRequest.getUserName()).thenReturn(DUMMY_USER_NAME);
        when(mockProduction.getName()).thenReturn(DUMMY_JOB_ID);
        when(mockProduction.getStagingPath()).thenReturn(DUMMY_PATH);
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        PowerMockito.when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.when(CalvalusProductionService.getServiceContainerSingleton()).thenReturn(mockServiceContainer);
        Logger mockLogger2 = mock(Logger.class);
        PowerMockito.mockStatic(CalvalusLogger.class);
        PowerMockito.when(CalvalusLogger.getLogger()).thenReturn(mockLogger2);

        ArgumentCaptor<String> statusCaptor = ArgumentCaptor.forClass(String.class);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.observeStagingStatus("jobId");

        verify(mockLogger2, times(1)).info(statusCaptor.capture());

        assertThat(statusCaptor.getValue(), equalTo("Staging completed."));
    }

    private ProcessStatus getProcessStatusStillRunning() {
        ProcessStatus mockProcessStatus = mock(ProcessStatus.class);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.RUNNING, ProcessState.RUNNING, ProcessState.COMPLETED);
        when(mockProcessStatus.getProgress()).thenReturn(0.8f, 0.9f, 1f);
        when(mockProcessStatus.getMessage()).thenReturn("Still running", "Still running", "Finished");
        when(mockProcessStatus.isDone()).thenReturn(false, false, true);
        return mockProcessStatus;
    }

    private ProcessStatus getProcessStatusAlreadyDone() {
        ProcessStatus mockProcessStatus = mock(ProcessStatus.class);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProcessStatus.getProgress()).thenReturn(1f);
        when(mockProcessStatus.getMessage()).thenReturn("Finished");
        when(mockProcessStatus.isDone()).thenReturn(true);
        return mockProcessStatus;
    }

    private Map<String, String> getMockDefaultConfig() {
        Map<String, String> mockCalvalusDefaultConfig = new HashMap<>();
        mockCalvalusDefaultConfig.put("calvalus.wps.staging.path", "staging");
        mockCalvalusDefaultConfig.put("calvalus.generate.metadata", "true");
        return mockCalvalusDefaultConfig;
    }

    private File[] getProductResultFiles() {
        List<File> productResultFileList = new ArrayList<>();

        File product1 = mock(File.class);
        when(product1.getName()).thenReturn("product1.nc");
        productResultFileList.add(product1);

        File product2 = mock(File.class);
        when(product2.getName()).thenReturn("product2.nc");
        productResultFileList.add(product2);

        return productResultFileList.toArray(new File[2]);
    }

}