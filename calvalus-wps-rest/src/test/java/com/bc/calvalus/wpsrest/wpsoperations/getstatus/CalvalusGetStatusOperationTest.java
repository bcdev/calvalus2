package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.FailedRequestException;
import com.bc.calvalus.wpsrest.exception.JobNotFoundException;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CalvalusGetStatusOperation.class, CalvalusFacade.class, CalvalusExecuteResponseConverter.class})
public class CalvalusGetStatusOperationTest {

    private CalvalusGetStatusOperation getStatusOperation;

    private WpsMetadata mockWpsMetadata;
    private CalvalusFacade mockCalvalusFacade;
    private ProductionService mockProductionService;
    private Production mockProduction;

    @Before
    public void setUp() throws Exception {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockCalvalusFacade = mock(CalvalusFacade.class);
        mockProductionService = mock(ProductionService.class);
        mockProduction = mock(Production.class);
    }

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Test
    public void canGetInProgressStatus() throws Exception {
        ProcessStatus mockInProgressStatus = getInProgressProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockCalvalusExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        ExecuteResponse emptyExecuteResponse = new ExecuteResponse();
        when(mockCalvalusExecuteResponse.getStartedResponse(anyString(), anyFloat())).thenReturn(emptyExecuteResponse);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockCalvalusExecuteResponse);

        ArgumentCaptor<String> processState = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Float> processProgress = ArgumentCaptor.forClass(Float.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        String getStatusResponse = getStatusOperation.getStatus();

        verify(mockCalvalusExecuteResponse).getStartedResponse(processState.capture(), processProgress.capture());
        assertThat(processState.getValue(), equalTo("RUNNING"));
        assertThat(processProgress.getValue(), equalTo(40.0F));
        assertThat(getStatusResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<ExecuteResponse xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\"/>\n"));
    }

    @Test
    public void canGetFailedStatus() throws Exception {
        ProcessStatus mockFailedStatus = getFailedProcessStatus();
        ProcessStatus mockIncompleteStagingStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockFailedStatus);
        when(mockProduction.getStagingStatus()).thenReturn(mockIncompleteStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockCalvalusExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        ExecuteResponse emptyExecuteResponse = new ExecuteResponse();
        when(mockCalvalusExecuteResponse.getFailedResponse(anyString())).thenReturn(emptyExecuteResponse);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockCalvalusExecuteResponse);

        ArgumentCaptor<String> processMessage = ArgumentCaptor.forClass(String.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        String getStatusResponse = getStatusOperation.getStatus();

        verify(mockCalvalusExecuteResponse).getFailedResponse(processMessage.capture());
        assertThat(processMessage.getValue(), equalTo("Error in processing the job"));
        assertThat(getStatusResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<ExecuteResponse xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\"/>\n"));
    }

    @Test
    public void canGetSuccessfulStatus() throws Exception {
        ProcessStatus mockCompletedStagingStatus = getDoneAndSuccessfulProcessStatus();
        when(mockProduction.getStagingStatus()).thenReturn(mockCompletedStagingStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        List<String> mockResultUrlList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(mockResultUrlList);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockCalvalusExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        ExecuteResponse emptyExecuteResponse = new ExecuteResponse();
        when(mockCalvalusExecuteResponse.getSuccessfulResponse(mockResultUrlList)).thenReturn(emptyExecuteResponse);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockCalvalusExecuteResponse);

        ArgumentCaptor<List> resultUrl = ArgumentCaptor.forClass(List.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        String getStatusResponse = getStatusOperation.getStatus();

        verify(mockCalvalusExecuteResponse).getSuccessfulResponse(resultUrl.capture());
        assertThat(resultUrl.getValue(), equalTo(mockResultUrlList));
        assertThat(getStatusResponse, equalTo("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<ExecuteResponse xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\"/>\n"));
    }

    @Test
    public void canGetExecuteAcceptedResponse() throws Exception {
        ProcessStatus mockInProgressStatus = getInProgressProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteResponse);

        ArgumentCaptor<String> processState = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Float> processProgress = ArgumentCaptor.forClass(Float.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        getStatusOperation.getExecuteAcceptedResponse();

        verify(mockExecuteResponse).getStartedResponse(processState.capture(), processProgress.capture());
        assertThat(processState.getValue(), equalTo("RUNNING"));
        assertThat(processProgress.getValue(), equalTo(40.0F));
    }

    @Test
    public void canGetExecuteFailedResponse() throws Exception {
        ProcessStatus mockFailedStatus = getFailedProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockFailedStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteResponse);

        ArgumentCaptor<String> processMessage = ArgumentCaptor.forClass(String.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        getStatusOperation.getExecuteFailedResponse();

        verify(mockExecuteResponse).getFailedResponse(processMessage.capture());
        assertThat(processMessage.getValue(), equalTo("Error in processing the job"));
    }

    @Test
    public void canGetExecuteSuccessfulResponse() throws Exception {
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        List<String> mockResultUrlList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(mockResultUrlList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);
        CalvalusExecuteResponseConverter mockExecuteResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteResponse);

        ArgumentCaptor<List> resultUrl = ArgumentCaptor.forClass(List.class);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        getStatusOperation.getExecuteSuccessfulResponse();

        verify(mockExecuteResponse).getSuccessfulResponse(resultUrl.capture());
        assertThat(resultUrl.getValue(), equalTo(mockResultUrlList));
    }

    @Test
    public void canDetermineIfProductionJobFinishedAndSuccessful() throws Exception {
        ProcessStatus mockInProgressStatus = getDoneAndSuccessfulProcessStatus();
        when(mockProduction.getStagingStatus()).thenReturn(mockInProgressStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        List<String> mockResultUrlList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(mockResultUrlList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");

        assertThat(getStatusOperation.isProductionJobFinishedAndSuccessful(), equalTo(true));
    }

    @Test
    public void canDetermineIfProductionJobFinishedAndFailed() throws Exception {
        ProcessStatus mockInProgressStatus = getDoneAndFailedProcessStatus();
        when(mockProduction.getProcessingStatus()).thenReturn(mockInProgressStatus);
        when(mockProductionService.getProduction(anyString())).thenReturn(mockProduction);
        when(mockCalvalusFacade.getProductionService()).thenReturn(mockProductionService);
        List<String> mockResultUrlList = new ArrayList<>();
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(mockResultUrlList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");

        assertThat(getStatusOperation.isProductionJobFinishedAndFailed(), equalTo(true));
    }

    @Test
    public void canCatchIOExceptionWhenCheckingJob() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class))
                    .thenThrow(new IOException("IOException error."));

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Unable to retrieve the job with jobId 'job-01'.");

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        getStatusOperation.isProductionJobFinishedAndFailed();
    }

    @Test
    public void canCatchProductionExceptionWhenCheckingJob() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class))
                    .thenThrow(new ProductionException("ProductionException error."));

        thrownException.expect(JobNotFoundException.class);
        thrownException.expectMessage("Unable to retrieve the job with jobId 'job-01'.");

        getStatusOperation = new CalvalusGetStatusOperation(mockWpsMetadata, "job-01");
        getStatusOperation.isProductionJobFinishedAndFailed();
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

    private ProcessStatus getDoneAndFailedProcessStatus() {
        ProcessStatus mockFailedStatus = mock(ProcessStatus.class);
        when(mockFailedStatus.getState()).thenReturn(ProcessState.ERROR);
        return mockFailedStatus;
    }

}