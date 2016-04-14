package com.bc.calvalus.wps.wpsoperations;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.responses.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;
import com.bc.wps.api.utils.WpsTypeConverter;
import com.bc.wps.utilities.PropertiesWrapper;
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
@PrepareForTest({
            CalvalusExecuteOperation.class, CalvalusFacade.class, ExecuteRequestExtractor.class,
            ProcessorNameParser.class, ProductionRequest.class, CalvalusDataInputs.class,
            CalvalusExecuteResponseConverter.class
})
public class CalvalusExecuteOperationTest {

    private CalvalusExecuteOperation executeOperation;

    private Execute mockExecuteRequest;
    private WpsRequestContext mockRequestContext;
    private WpsServerContext mockServerContext;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockExecuteRequest = mock(Execute.class);
        mockRequestContext = mock(WpsRequestContext.class);
        mockServerContext = mock(WpsServerContext.class);
        when(mockServerContext.getHostAddress()).thenReturn("dummyHostName");
        when(mockServerContext.getRequestUrl()).thenReturn("http://dummyUrl.com/bc-wps/wps/provider1");
        when(mockServerContext.getPort()).thenReturn(8000);
        when(mockRequestContext.getServerContext()).thenReturn(mockServerContext);
    }

    @Test
    public void canExecuteAsync() throws Exception {
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        when(mockResponseDocument.isStatus()).thenReturn(true);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        when(mockExecuteRequest.getIdentifier()).thenReturn(WpsTypeConverter.str2CodeType("process1"));
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("process-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        ExecuteResponse executeResponse = executeOperation.execute(mockExecuteRequest);

        assertThat(executeResponse.getStatus().getProcessAccepted(),
                   equalTo("The request has been accepted. The status of the process can be found in the URL."));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://dummyUrl.com/bc-wps/wps/provider1?Service=WPS&Request=GetStatus&JobId=process-00"));
        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("process1"));
    }

    @Test
    public void canExecuteSync() throws Exception {
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        when(mockResponseDocument.isStatus()).thenReturn(false);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        when(mockExecuteRequest.getIdentifier()).thenReturn(WpsTypeConverter.str2CodeType("process1"));
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("process-00");
        when(mockCalvalusFacade.orderProductionSynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        List<String> resultUrlList = new ArrayList<>();
        resultUrlList.add("resultUrl1");
        resultUrlList.add("resultUrl2");
        when(mockCalvalusFacade.getProductResultUrls(any(Production.class))).thenReturn(resultUrlList);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        ExecuteResponse executeResponse = executeOperation.execute(mockExecuteRequest);

        assertThat(executeResponse.getStatus().getProcessSucceeded(), equalTo("The request has been processed successfully."));
        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("process1"));
        assertThat(executeResponse.getProcessOutputs().getOutput().size(), equalTo(2));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(), equalTo("resultUrl1"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(), equalTo("resultUrl2"));
    }

    @Test
    public void canProcessSync() throws Exception {
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        ProductionRequest mockProductionRequest = configureProcessingMock();

        ArgumentCaptor<Execute> executeRequestCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> processIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductionRequest> productionRequestCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.processSync(mockExecuteRequest, "process1");

        PowerMockito.verifyNew(ExecuteRequestExtractor.class).withArguments(executeRequestCaptor.capture());
        PowerMockito.verifyNew(ProcessorNameParser.class).withArguments(processIdCaptor.capture());
        verify(mockCalvalusFacade).getProcessor(any(ProcessorNameParser.class));
        verify(mockCalvalusFacade).orderProductionSynchronous(productionRequestCaptor.capture());
        verify(mockCalvalusFacade).stageProduction(any(Production.class));
        verify(mockCalvalusFacade).observeStagingStatus(any(Production.class));
        verify(mockCalvalusFacade).getProductResultUrls(any(Production.class));

        assertThat(executeRequestCaptor.getValue(), equalTo(mockExecuteRequest));
        assertThat(processIdCaptor.getValue(), equalTo("process1"));
        assertThat(productionRequestCaptor.getValue(), equalTo(mockProductionRequest));
    }

    @Test
    public void canThrowFailedRequestExceptionInProcessSync() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenThrow(new InterruptedException("error"));

        thrownException.expect(InterruptedException.class);
        thrownException.expectMessage("error");

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.processSync(mockExecuteRequest, "process1");
    }

    @Test
    public void canProcessAsync() throws Exception {
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("process-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);
        ProductionRequest mockProductionRequest = configureProcessingMock();

        ArgumentCaptor<Execute> executeRequestCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<String> processIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductionRequest> productionRequestCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        String jobId = executeOperation.processAsync(mockExecuteRequest, "process1");

        PowerMockito.verifyNew(ExecuteRequestExtractor.class).withArguments(executeRequestCaptor.capture());
        PowerMockito.verifyNew(ProcessorNameParser.class).withArguments(processIdCaptor.capture());
        verify(mockCalvalusFacade).getProcessor(any(ProcessorNameParser.class));
        verify(mockCalvalusFacade).orderProductionAsynchronous(productionRequestCaptor.capture());

        assertThat(executeRequestCaptor.getValue(), equalTo(mockExecuteRequest));
        assertThat(processIdCaptor.getValue(), equalTo("process1"));
        assertThat(productionRequestCaptor.getValue(), equalTo(mockProductionRequest));
        assertThat(jobId, equalTo("process-00"));
    }

    @Test
    public void canCatchIOExceptionInProcessAsync() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenThrow(new IOException("error"));

        thrownException.expect(IOException.class);
        thrownException.expectMessage("error");

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.processAsync(mockExecuteRequest, "process1");
    }

    @Test
    public void canCreateLineageAsyncResponse() throws Exception {
        CalvalusExecuteResponseConverter mockExecuteAcceptedResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteAcceptedResponse);
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        List<DocumentOutputDefinitionType> mockOutputList = new ArrayList<>();
        when(mockResponseDocument.getOutput()).thenReturn(mockOutputList);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        DataInputsType mockDataInputs = mock(DataInputsType.class);
        when(mockExecuteRequest.getDataInputs()).thenReturn(mockDataInputs);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DataInputsType> dataInputsCaptor = ArgumentCaptor.forClass(DataInputsType.class);
        ArgumentCaptor<List> output = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<WpsServerContext> serverContextCaptor = ArgumentCaptor.forClass(WpsServerContext.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.createAsyncExecuteResponse(mockExecuteRequest, true, "process1");

        verify(mockExecuteAcceptedResponse).getAcceptedWithLineageResponse(jobIdCaptor.capture(), dataInputsCaptor.capture(),
                                                                           output.capture(), serverContextCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("process1"));
        assertThat(dataInputsCaptor.getValue(), equalTo(mockDataInputs));
        assertThat(output.getValue(), equalTo(mockOutputList));
        assertThat(serverContextCaptor.getValue(), equalTo(mockServerContext));
    }

    @Test
    public void canCreateNonLineageAsyncResponse() throws Exception {
        CalvalusExecuteResponseConverter mockExecuteAcceptedResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteAcceptedResponse);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<WpsServerContext> serverContextCaptor = ArgumentCaptor.forClass(WpsServerContext.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.createAsyncExecuteResponse(mockExecuteRequest, false, "process1");

        verify(mockExecuteAcceptedResponse).getAcceptedResponse(jobIdCaptor.capture(), serverContextCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("process1"));
        assertThat(serverContextCaptor.getValue(), equalTo(mockServerContext));
    }

    @Test
    public void canCreateLineageSyncResponse() throws Exception {
        CalvalusExecuteResponseConverter mockExecuteAcceptedResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteAcceptedResponse);
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        List<DocumentOutputDefinitionType> mockOutputList = new ArrayList<>();
        when(mockResponseDocument.getOutput()).thenReturn(mockOutputList);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        DataInputsType mockDataInputs = mock(DataInputsType.class);
        when(mockExecuteRequest.getDataInputs()).thenReturn(mockDataInputs);
        List<String> mockResultUrls = new ArrayList<>();

        ArgumentCaptor<List> resultUrlsCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<DataInputsType> dataInputsCaptor = ArgumentCaptor.forClass(DataInputsType.class);
        ArgumentCaptor<List> output = ArgumentCaptor.forClass(List.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.createSyncExecuteResponse(mockExecuteRequest, true, mockResultUrls);

        verify(mockExecuteAcceptedResponse).getSuccessfulWithLineageResponse(resultUrlsCaptor.capture(), dataInputsCaptor.capture(),
                                                                             output.capture());

        assertThat(resultUrlsCaptor.getValue(), equalTo(mockResultUrls));
        assertThat(dataInputsCaptor.getValue(), equalTo(mockDataInputs));
        assertThat(output.getValue(), equalTo(mockOutputList));
    }

    @Test
    public void canCreateNonLineageSyncResponse() throws Exception {
        CalvalusExecuteResponseConverter mockExecuteAcceptedResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteAcceptedResponse);
        List<String> mockResultUrls = new ArrayList<>();

        ArgumentCaptor<List> resultUrlsCaptor = ArgumentCaptor.forClass(List.class);

        executeOperation = new CalvalusExecuteOperation(mockRequestContext);
        executeOperation.createSyncExecuteResponse(mockExecuteRequest, false, mockResultUrls);

        verify(mockExecuteAcceptedResponse).getSuccessfulResponse(resultUrlsCaptor.capture());

        assertThat(resultUrlsCaptor.getValue(), equalTo(mockResultUrls));
    }

    private ProductionRequest configureProcessingMock() throws Exception {
        ExecuteRequestExtractor mockRequestExtractor = mock(ExecuteRequestExtractor.class);
        PowerMockito.whenNew(ExecuteRequestExtractor.class).withArguments(any(Execute.class)).thenReturn(mockRequestExtractor);
        ProcessorNameParser mockParser = mock(ProcessorNameParser.class);
        PowerMockito.whenNew(ProcessorNameParser.class).withArguments(anyString()).thenReturn(mockParser);
        CalvalusDataInputs mockCalvalusDataInputs = mock(CalvalusDataInputs.class);
        PowerMockito.whenNew(CalvalusDataInputs.class).withAnyArguments().thenReturn(mockCalvalusDataInputs);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        PowerMockito.whenNew(ProductionRequest.class).withAnyArguments().thenReturn(mockProductionRequest);
        return mockProductionRequest;
    }

}