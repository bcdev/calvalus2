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
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.exceptions.JobNotFoundException;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.exceptions.InvalidParameterValueException;
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
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusGetStatusOperation.class, CalvalusFacade.class})
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
    }

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Test
    public void canGetInProgressStatus() throws Exception {
        ProcessStatus mockInProgressStatus = getInProgressProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        when(mockCalvalusFacade.getServices()).thenReturn(mockServiceContainer);
        when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
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
    public void canCatchIOExceptionWhenGetInProgressStatus() throws Exception {
        ProcessStatus mockInProgressStatus = getInProgressProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getServices()).thenThrow(new IOException("IOException error"));
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Parameter 'JobId' has an invalid value.");
        thrownException.expect(instanceOf(InvalidParameterValueException.class));

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        getStatusOperation.getStatus("job-01");
    }

    @Test
    public void canCatchProductionExceptionWhenGetInProgressStatus() throws Exception {
        ProcessStatus mockInProgressStatus = getInProgressProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getServices()).thenThrow(new IOException("Production error"));
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Parameter 'JobId' has an invalid value.");

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        getStatusOperation.getStatus("job-01");
    }

    @Test
    public void canGetFailedStatus() throws Exception {
        ProcessStatus mockFailedStatus = getFailedProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProduction.getProcessingStatus()).thenReturn(mockFailedStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        when(mockCalvalusFacade.getServices()).thenReturn(mockServiceContainer);
        when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
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
    public void canGetSuccessfulStatus() throws Exception {
        ProcessStatus mockCompletedStagingStatus = getDoneAndSuccessfulProcessStatus();
        ProductionRequest mockProductionRequest = getMockProductionRequest();
        when(mockProduction.getProductionRequest()).thenReturn(mockProductionRequest);
        when(mockProduction.getStagingStatus()).thenReturn(mockCompletedStagingStatus);
        WorkflowItem mockWorkflow = mock(WorkflowItem.class);
        when(mockWorkflow.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockProduction.getWorkflow()).thenReturn(mockWorkflow);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        List<String> mockResultUrlList = new ArrayList<>();
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc");
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip");
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(mockResultUrlList);
        ServiceContainer mockServiceContainer = mock(ServiceContainer.class);
        when(mockCalvalusFacade.getServices()).thenReturn(mockServiceContainer);
        when(mockServiceContainer.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        Calendar calendar = Calendar.getInstance();

        getStatusOperation = new CalvalusGetStatusOperation(mockRequestContext);
        ExecuteResponse getStatusResponse = getStatusOperation.getStatus("job-01");

        assertThat(getStatusResponse.getProcess().getIdentifier().getValue(), equalTo("mockBundle~1.0~mockProcessor"));
        assertThat(getStatusResponse.getProcess().getProcessVersion(), equalTo("1.0"));
        assertThat(getStatusResponse.getStatus().getCreationTime().toString(), equalTo("2016-01-01T01:00:00.000+01:00"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(getStatusResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));
    }

    private ProductionRequest getMockProductionRequest() throws ProductionException {
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        when(mockProductionRequest.getString(PROCESSOR_BUNDLE_NAME.getIdentifier())).thenReturn("mockBundle");
        when(mockProductionRequest.getString(PROCESSOR_BUNDLE_VERSION.getIdentifier())).thenReturn("1.0");
        when(mockProductionRequest.getString(PROCESSOR_NAME.getIdentifier())).thenReturn("mockProcessor");
        return mockProductionRequest;
    }

    private ProcessStatus getInProgressProcessStatus() {
        ProcessStatus mockInProgressStatus = mock(ProcessStatus.class);
        when(mockInProgressStatus.getState()).thenReturn(ProcessState.RUNNING);
        when(mockInProgressStatus.getProgress()).thenReturn(0.4f);
        return mockInProgressStatus;
    }

    private ProcessStatus getFailedProcessStatus() {
        ProcessStatus mockFailedStatus = mock(ProcessStatus.class);
        when(mockFailedStatus.getMessage()).thenReturn("Error in processing the job");
        when(mockFailedStatus.getState()).thenReturn(ProcessState.ERROR);
        return mockFailedStatus;
    }

    private ProcessStatus getDoneAndSuccessfulProcessStatus() {
        ProcessStatus mockSuccessfulStatus = mock(ProcessStatus.class);
        when(mockSuccessfulStatus.getState()).thenReturn(ProcessState.COMPLETED);
        return mockSuccessfulStatus;
    }
}