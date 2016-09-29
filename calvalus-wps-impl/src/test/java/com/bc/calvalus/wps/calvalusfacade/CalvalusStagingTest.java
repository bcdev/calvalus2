package com.bc.calvalus.wps.calvalusfacade;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.api.WpsServerContext;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusStaging.class, File.class, Thread.class, CalvalusLogger.class,
            ProductMetadata.class, VelocityWrapper.class
})
public class CalvalusStagingTest {

    private WpsServerContext mockServerContext;
    private Production mockProduction;
    private ProductionService mockProductionService;
    private Logger mockLogger;

    /**
     * Class under test.
     */
    private CalvalusStaging calvalusStaging;

    @Before
    public void setUp() throws Exception {
        mockServerContext = mock(WpsServerContext.class);
        mockProduction = mock(Production.class);
        mockProductionService = mock(ProductionService.class);
        mockLogger = mock(Logger.class);

        when(mockServerContext.getHostAddress()).thenReturn("calvalustomcat-test");
        when(mockServerContext.getPort()).thenReturn(8080);

        PowerMockito.mockStatic(Thread.class);
        PowerMockito.mockStatic(CalvalusLogger.class);
        PowerMockito.when(CalvalusLogger.getLogger()).thenReturn(mockLogger);
    }

    @Test
    public void testStageProduction() throws Exception {
        when(mockProduction.getId()).thenReturn("productId");
        ArgumentCaptor<String> productId = ArgumentCaptor.forClass(String.class);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.stageProduction(mockProductionService, "productId");

        verify(mockProductionService).stageProductions(productId.capture());
        assertThat(productId.getValue(), equalTo("productId"));
    }

    // TODO include more mocking in the test so that it passes
    @Ignore
    @Test
    public void canGetProductResultUrls() throws Exception {
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();
        when(mockProduction.getStagingPath()).thenReturn("20150915103935_L3_173a941e1ceb0/L3_2009-06-01_2009-06-30.nc");
        File mockStagingDirectory = mock(File.class);
        File[] mockProductResultFiles = getProductResultFiles();
        when(mockStagingDirectory.listFiles()).thenReturn(mockProductResultFiles);
        PowerMockito.whenNew(File.class).withArguments(anyString()).thenReturn(mockStagingDirectory);
        ProductMetadata mockProductMetadata = mock(ProductMetadata.class);
        PowerMockito.whenNew(ProductMetadata.class).withAnyArguments().thenReturn(mockProductMetadata);
        VelocityWrapper mockWrapper = mock(VelocityWrapper.class);
        when(mockWrapper.merge(anyMapOf(String.class, Object.class), anyString())).thenReturn("merged string");
        PowerMockito.whenNew(VelocityWrapper.class).withNoArguments().thenReturn(mockWrapper);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        List<String> productResultUrls = calvalusStaging.getProductResultUrls(mockCalvalusDefaultConfig, mockProduction);

        assertThat(productResultUrls.size(), equalTo(2));
        assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/20150915103935_L3_173a941e1ceb0/L3_2009-06-01_2009-06-30.nc/product1.nc"));
        assertThat(productResultUrls, hasItems("http://calvalustomcat-test:8080/bc-wps/staging/20150915103935_L3_173a941e1ceb0/L3_2009-06-01_2009-06-30.nc/product2.nc"));
    }

    @Test
    public void canGetNullProductResultUrls() throws Exception {
        Map<String, String> mockCalvalusDefaultConfig = getMockDefaultConfig();
        when(mockProduction.getStagingPath()).thenReturn("localhost:9080/staging/calvalustest/20150915103935_L3_173a941e1ceb0/L3_2009-06-01_2009-06-30.nc");
        File mockStagingDirectory = mock(File.class);
        when(mockStagingDirectory.listFiles()).thenReturn(null);
        PowerMockito.whenNew(File.class).withArguments(anyString()).thenReturn(mockStagingDirectory);

        calvalusStaging = new CalvalusStaging(mockServerContext);
        List<String> productResultUrls = calvalusStaging.getProductResultUrls(mockCalvalusDefaultConfig, mockProduction);

        assertThat(productResultUrls.size(), equalTo(0));
    }

    @PrepareForTest({CalvalusStaging.class, Thread.class, CalvalusLogger.class})
    @Test
    public void testObserveStagingStatusInitiallyNotDoneAndThenCompleted() throws Exception {
        ArgumentCaptor<String> logMessage = ArgumentCaptor.forClass(String.class);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getUserName()).thenReturn("dummyUserName");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        ProcessStatus mockProcessStatus = mock(ProcessStatus.class);
        when(mockProcessStatus.isDone()).thenReturn(false, false, true);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.COMPLETED);
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        CalvalusStaging calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.observeStagingStatus(mockProductionService, mockProduction);

        verify(mockProductionService, times(2)).updateStatuses(anyString());
        verify(mockLogger, times(3)).info(logMessage.capture());
        assertThat(logMessage.getAllValues().get(2), equalTo("Staging completed."));
    }

    @PrepareForTest({CalvalusStaging.class, Thread.class, CalvalusLogger.class})
    @Test
    public void testObserveStagingStatusInitiallyNotDoneAndThenFailed() throws Exception {
        ArgumentCaptor<String> logMessage = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Level> errorLevel = ArgumentCaptor.forClass(Level.class);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getUserName()).thenReturn("dummyUserName");
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        ProcessStatus mockProcessStatus = mock(ProcessStatus.class);
        when(mockProcessStatus.isDone()).thenReturn(false, false, true);
        when(mockProcessStatus.getState()).thenReturn(ProcessState.ERROR);
        when(mockProcessStatus.getMessage()).thenReturn("error message");
        when(mockProduction.getStagingStatus()).thenReturn(mockProcessStatus);

        CalvalusStaging calvalusStaging = new CalvalusStaging(mockServerContext);
        calvalusStaging.observeStagingStatus(mockProductionService, mockProduction);

        verify(mockProductionService, times(2)).updateStatuses(anyString());
        verify(mockLogger, times(1)).log(errorLevel.capture(), logMessage.capture());
        assertThat(logMessage.getAllValues().get(0), equalTo("Error: Staging did not complete normally: error message"));
    }

    private Map<String, String> getMockDefaultConfig() {
        Map<String, String> mockCalvalusDefaultConfig = new HashMap<>();
        mockCalvalusDefaultConfig.put("calvalus.wps.staging.path", "staging");
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