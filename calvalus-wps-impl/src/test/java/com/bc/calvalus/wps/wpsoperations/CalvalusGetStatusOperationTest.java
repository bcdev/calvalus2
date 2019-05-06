package com.bc.calvalus.wps.wpsoperations;

import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_NAME;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_BUNDLE_VERSION;
import static com.bc.calvalus.wps.calvalusfacade.CalvalusParameter.PROCESSOR_NAME;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.calvalusfacade.CalvalusWpsProcessStatus;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.calvalus.wps.localprocess.GpfProductionService;
import com.bc.calvalus.wps.localprocess.LocalJob;
import com.bc.calvalus.wps.localprocess.LocalProductionService;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.localprocess.WpsProcessStatus;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusGetStatusOperation.class, CalvalusFacade.class,
            WpsProcessStatus.class, GpfProductionService.class
})
public class CalvalusGetStatusOperationTest {

    private CalvalusGetStatusOperation getStatusOperation;

    private CalvalusFacade mockCalvalusFacade;
    private ProductionService mockProductionService;
    private Production mockProduction;
    private WpsRequestContext mockRequestContext;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockCalvalusFacade = mock(CalvalusFacade.class);
        mockProductionService = mock(ProductionService.class);
        mockProduction = mock(Production.class);
        mockRequestContext = mock(WpsRequestContext.class);
        WpsServerContext mockServerContext = mock(WpsServerContext.class);
        when(mockRequestContext.getServerContext()).thenReturn(mockServerContext);
    }

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Test
    public void canGetInProgressStatus() throws Exception {
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockCalvalusFacade.getProduction(anyString())).thenReturn(mockProduction);
        ArrayList<String> dummyList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls("job-01")).thenReturn(dummyList);
        CalvalusWpsProcessStatus mockStatus = getInProgressProcessStatus();
        PowerMockito.whenNew(CalvalusWpsProcessStatus.class).withArguments(mockProduction, dummyList).thenReturn(mockStatus);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        Calendar calendar = Calendar.getInstance();

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse executeResponse = getStatusOperation.getStatus("job-01");

        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("mockBundle~1.0~mockProcessor"));
        assertThat(executeResponse.getProcess().getProcessVersion(), equalTo("1.0"));
        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));

        assertThat(executeResponse.getStatus().getProcessStarted().getPercentCompleted(), equalTo(40));
        assertThat(executeResponse.getStatus().getProcessStarted().getValue(), equalTo("RUNNING"));
    }

    @Test
    public void canGetInProgressStatusLocalProcess() throws Exception {
        LocalProductionStatus mockStatus = getInProgressLocalProcessStatus();
        PowerMockito.mockStatic(GpfProductionService.class);
        HashMap<String, Object> mockJobParameters = getMockJobParameters();
        LocalJob mockJob = mock(LocalJob.class);
        when(mockJob.getStatus()).thenReturn(mockStatus);
        when(mockJob.getParameters()).thenReturn(mockJobParameters);
        LocalProductionService mockLocalProductionService = mock(LocalProductionService.class);
        when(mockLocalProductionService.getJob("urban1-20160919_160202392")).thenReturn(mockJob);
        PowerMockito.when(GpfProductionService.getProductionServiceSingleton()).thenReturn(mockLocalProductionService);
        Calendar calendar = Calendar.getInstance();

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse executeResponse = getStatusOperation.getStatus("urban1-20160919_160202392");

        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("local~0.0.1~Subset"));
        assertThat(executeResponse.getProcess().getProcessVersion(), equalTo("0.0.1"));
        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));

        assertThat(executeResponse.getStatus().getProcessStarted().getPercentCompleted(), equalTo(40));
        assertThat(executeResponse.getStatus().getProcessStarted().getValue(), equalTo("RUNNING"));
    }

    @Test
    public void canThrowJobNotFoundExceptionWhenProductionNull() throws Exception {
        when(mockCalvalusFacade.getProduction(anyString())).thenReturn(null);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Parameter 'JobId job-01' has an invalid value.");

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        getStatusOperation.getStatus("job-01");
    }

    @Test
    public void canCatchProductionExceptionWhenGetInProgressStatus() throws Exception {
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProduction(anyString())).thenThrow(new IOException("Production error"));
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Parameter 'JobId job-01' has an invalid value.");

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        getStatusOperation.getStatus("job-01");
    }

    @Test
    public void canGetFailedStatus() throws Exception {
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockCalvalusFacade.getProduction(anyString())).thenReturn(mockProduction);
        ArrayList<String> dummyList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls("job-01")).thenReturn(dummyList);
        CalvalusWpsProcessStatus mockStatus = getFailedProcessStatus();
        PowerMockito.whenNew(CalvalusWpsProcessStatus.class).withArguments(mockProduction, dummyList).thenReturn(mockStatus);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        Calendar calendar = Calendar.getInstance();

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse getStatusResponse = getStatusOperation.getStatus("job-01");

        assertThat(getStatusResponse.getProcess().getIdentifier().getValue(), equalTo("mockBundle~1.0~mockProcessor"));
        assertThat(getStatusResponse.getProcess().getProcessVersion(), equalTo("1.0"));
        assertThat(getStatusResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(getStatusResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(getStatusResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getVersion(), equalTo("1.0.0"));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionCode(),
                   equalTo("NoApplicableCode"));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionText().get(0),
                   equalTo("Error in processing the job"));

    }

    @Test
    public void canGetFailedStatusLocalProcess() throws Exception {
        LocalProductionStatus mockStatus = getFailedLocalProcessStatus();
        PowerMockito.mockStatic(GpfProductionService.class);
        HashMap<String, Object> mockJobParameters = getMockJobParameters();
        LocalJob mockJob = mock(LocalJob.class);
        when(mockJob.getStatus()).thenReturn(mockStatus);
        when(mockJob.getParameters()).thenReturn(mockJobParameters);
        LocalProductionService mockLocalProductionService = mock(LocalProductionService.class);
        when(mockLocalProductionService.getJob("urban1-20160919_160202392")).thenReturn(mockJob);
        PowerMockito.when(GpfProductionService.getProductionServiceSingleton()).thenReturn(mockLocalProductionService);
        Calendar calendar = Calendar.getInstance();

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse getStatusResponse = getStatusOperation.getStatus("urban1-20160919_160202392");

        assertThat(getStatusResponse.getProcess().getIdentifier().getValue(), equalTo("local~0.0.1~Subset"));
        assertThat(getStatusResponse.getProcess().getProcessVersion(), equalTo("0.0.1"));
        assertThat(getStatusResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(getStatusResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(getStatusResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getVersion(), equalTo("1.0.0"));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionCode(),
                   equalTo("NoApplicableCode"));
        assertThat(getStatusResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionText().get(0),
                   equalTo("Error in processing the job"));

    }

    @Test
    public void canGetSuccessfulStatus() throws Exception {
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockCalvalusFacade.getProduction(anyString())).thenReturn(mockProduction);
        List<String> mockResultUrlList = getMockResultUrlList();
        when(mockCalvalusFacade.getProductResultUrls("job-01")).thenReturn(mockResultUrlList);
        CalvalusWpsProcessStatus mockStatus = getDoneAndSuccessfulProcessStatus(mockResultUrlList);
        PowerMockito.whenNew(CalvalusWpsProcessStatus.class).withArguments(mockProduction, mockResultUrlList).thenReturn(mockStatus);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse getStatusResponse = getStatusOperation.getStatus("job-01");

        assertThat(getStatusResponse.getProcess().getIdentifier().getValue(), equalTo("mockBundle~1.0~mockProcessor"));
        assertThat(getStatusResponse.getProcess().getProcessVersion(), equalTo("1.0"));
        assertThat(getStatusResponse.getStatus().getCreationTime().toString(), equalTo("2016-01-01T01:00:00.000+01:00"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(),
                   equalTo("production_result"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("production_result"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));
    }

    @Test
    public void canGetSuccessfulStatusLocalProcess() throws Exception {
        List<String> dummyList = getMockResultUrlList();
        when(mockCalvalusFacade.getProductResultUrls("urban1-20160919_160202392")).thenReturn(dummyList);
        LocalProductionStatus mockStatus = getDoneAndSuccessfulLocalProcessStatus(dummyList);
        PowerMockito.mockStatic(GpfProductionService.class);
        HashMap<String, Object> mockJobParameters = getMockJobParameters();
        LocalJob mockJob = mock(LocalJob.class);
        when(mockJob.getStatus()).thenReturn(mockStatus);
        when(mockJob.getParameters()).thenReturn(mockJobParameters);
        LocalProductionService mockLocalProductionService = mock(LocalProductionService.class);
        when(mockLocalProductionService.getJob("urban1-20160919_160202392")).thenReturn(mockJob);
        PowerMockito.when(GpfProductionService.getProductionServiceSingleton()).thenReturn(mockLocalProductionService);

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse getStatusResponse = getStatusOperation.getStatus("urban1-20160919_160202392");

        assertThat(getStatusResponse.getProcess().getIdentifier().getValue(), equalTo("local~0.0.1~Subset"));
        assertThat(getStatusResponse.getProcess().getProcessVersion(), equalTo("0.0.1"));
        assertThat(getStatusResponse.getStatus().getCreationTime().toString(), equalTo("2016-01-01T01:00:00.000+01:00"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(),
                   equalTo("production_result"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("production_result"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));
    }

    @Test
    public void canThrowExceptionWhenJobUnavailableLocalProcess() throws Exception {
        PowerMockito.mockStatic(GpfProductionService.class);
        HashMap<String, LocalJob> statusMap = new HashMap<>();
        LocalProductionService mockLocalProductionService = mock(LocalProductionService.class);
        PowerMockito.when(GpfProductionService.getProductionServiceSingleton()).thenReturn(mockLocalProductionService);

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Parameter 'JobId urban1-20160919_160202392' has an invalid value.");

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        getStatusOperation.getStatus("urban1-20160919_160202392");
    }

    private HashMap<String, Object> getMockJobParameters() {
        HashMap<String, Object> jobParameters = new HashMap<>();
        jobParameters.put("processId", "local~0.0.1~Subset");
        return jobParameters;
    }

    private List<String> getMockResultUrlList() {
        List<String> mockResultUrlList = new ArrayList<>();
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc");
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip");
        return mockResultUrlList;
    }

    private ProductionRequest getMockProductionRequest() throws ProductionException {
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getString(PROCESSOR_BUNDLE_NAME.getIdentifier())).thenReturn("mockBundle");
        when(mockProductionRequest.getString(PROCESSOR_BUNDLE_VERSION.getIdentifier())).thenReturn("1.0");
        when(mockProductionRequest.getString(PROCESSOR_NAME.getIdentifier())).thenReturn("mockProcessor");
        when(mockProductionRequest.getString(PROCESSOR_NAME.getIdentifier(), "ra")).thenReturn("mockProcessor");
        return mockProductionRequest;
    }

    private CalvalusWpsProcessStatus getInProgressProcessStatus() {
        CalvalusWpsProcessStatus mockStatus = mock(CalvalusWpsProcessStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.RUNNING.toString());
        when(mockStatus.getProgress()).thenReturn(40f);
        return mockStatus;
    }

    private CalvalusWpsProcessStatus getFailedProcessStatus() {
        CalvalusWpsProcessStatus mockStatus = mock(CalvalusWpsProcessStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.ERROR.toString());
        when(mockStatus.getMessage()).thenReturn("Error in processing the job");
        when(mockStatus.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockStatus.isDone()).thenReturn(true);
        return mockStatus;
    }

    private CalvalusWpsProcessStatus getDoneAndSuccessfulProcessStatus(List<String> mockResultUrlList) {
        CalvalusWpsProcessStatus mockStatus = mock(CalvalusWpsProcessStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.COMPLETED.toString());
        when(mockStatus.getResultUrls()).thenReturn(mockResultUrlList);
        when(mockStatus.getStopTime()).thenReturn(new Date(1451606400000L));
        return mockStatus;
    }

    private LocalProductionStatus getInProgressLocalProcessStatus() {
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.RUNNING.toString());
        when(mockStatus.getProgress()).thenReturn(40f);
        return mockStatus;
    }

    private LocalProductionStatus getFailedLocalProcessStatus() {
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.ERROR.toString());
        when(mockStatus.getMessage()).thenReturn("Error in processing the job");
        when(mockStatus.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockStatus.isDone()).thenReturn(true);
        return mockStatus;
    }

    private LocalProductionStatus getDoneAndSuccessfulLocalProcessStatus(List<String> mockResultUrlList) {
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        when(mockStatus.getState()).thenReturn(ProcessState.COMPLETED.toString());
        when(mockStatus.getResultUrls()).thenReturn(mockResultUrlList);
        when(mockStatus.getStopTime()).thenReturn(new Date(1451606400000L));
        return mockStatus;
    }
}