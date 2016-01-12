package com.bc.calvalus.wpsrest.wpsoperations.execute;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wpsrest.ExecuteRequestExtractor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wpsrest.exception.WpsRuntimeException;
import com.bc.calvalus.wpsrest.jaxb.DataInputsType;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.jaxb.ResponseFormType;
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
@PrepareForTest({
            CalvalusExecuteOperation.class, CalvalusFacade.class, ExecuteRequestExtractor.class,
            ProcessorNameParser.class, ProductionRequest.class, CalvalusDataInputs.class,
            CalvalusExecuteResponseConverter.class
})
public class CalvalusExecuteOperationTest {

    private CalvalusExecuteOperation executeOperation;

    private WpsMetadata mockWpsMetadata;
    private ServletRequestWrapper mockServletRequestWrapper;
    private Execute mockExecuteRequest;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        when(mockWpsMetadata.getServletRequestWrapper()).thenReturn(mockServletRequestWrapper);
        mockExecuteRequest = mock(Execute.class);
    }

    @Test
    public void canExecuteAsync() throws Exception {
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        when(mockResponseDocument.isStatus()).thenReturn(true);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("proces-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);

        configureMockRequestUrl();

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        String executeResponse = executeOperation.execute();

        assertThat(executeResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                   "<ExecuteResponse statusLocation=\"http://dummyUrl.com?Service=WPS&amp;Request=GetStatus&amp;JobId=proces-00\" service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n"));
        assertThat(executeResponse, containsString("        <ProcessAccepted>The request has been accepted. The status of the process can be found in the URL.</ProcessAccepted>\n" +
                                                   "    </Status>\n" +
                                                   "</ExecuteResponse>\n"));
    }

    @Test
    public void canExecuteSync() throws Exception {
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        when(mockResponseDocument.isStatus()).thenReturn(false);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("proces-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(ServletRequestWrapper.class)).thenReturn(mockCalvalusFacade);

        configureMockRequestUrl();

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        String executeResponse = executeOperation.execute();

        assertThat(executeResponse, containsString("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                                                   "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">"));
        assertThat(executeResponse, containsString("        <ProcessSucceeded>The request has been processed successfully.</ProcessSucceeded>\n" +
                                                   "    </Status>\n" +
                                                   "    <ProcessOutputs/>\n" +
                                                   "</ExecuteResponse>\n"));
    }

    @Test
    public void canProcessSync() throws Exception {
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);
        ProductionRequest mockProductionRequest = configureProcessingMock();

        ArgumentCaptor<Execute> executeRequestCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<ServletRequestWrapper> servletRequestWrapperCaptor = ArgumentCaptor.forClass(ServletRequestWrapper.class);
        ArgumentCaptor<String> processIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductionRequest> productionRequestCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        executeOperation.processSync(mockExecuteRequest, "process1", mockWpsMetadata);

        PowerMockito.verifyNew(ExecuteRequestExtractor.class).withArguments(executeRequestCaptor.capture());
        PowerMockito.verifyNew(CalvalusFacade.class).withArguments(servletRequestWrapperCaptor.capture());
        PowerMockito.verifyNew(ProcessorNameParser.class).withArguments(processIdCaptor.capture());
        verify(mockCalvalusFacade).getProcessor(any(ProcessorNameParser.class));
        verify(mockCalvalusFacade).orderProductionSynchronous(productionRequestCaptor.capture());
        verify(mockCalvalusFacade).stageProduction(any(Production.class));
        verify(mockCalvalusFacade).observeStagingStatus(any(Production.class));
        verify(mockCalvalusFacade).getProductResultUrls(any(Production.class));

        assertThat(executeRequestCaptor.getValue(), equalTo(mockExecuteRequest));
        assertThat(servletRequestWrapperCaptor.getValue(), equalTo(mockServletRequestWrapper));
        assertThat(processIdCaptor.getValue(), equalTo("process1"));
        assertThat(productionRequestCaptor.getValue(), equalTo(mockProductionRequest));
    }

    @Test
    public void canCatchInterruptedExceptionInProcessSync() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new InterruptedException("error"));

        thrownException.expect(WpsRuntimeException.class);
        thrownException.expectMessage("Unable to process the request synchronously");

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        executeOperation.processSync(mockExecuteRequest, "process1", mockWpsMetadata);
    }

    @Test
    public void canProcessAsync() throws Exception {
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        when(mockProduction.getId()).thenReturn("proces-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(ProductionRequest.class))).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenReturn(mockCalvalusFacade);
        ProductionRequest mockProductionRequest = configureProcessingMock();

        ArgumentCaptor<Execute> executeRequestCaptor = ArgumentCaptor.forClass(Execute.class);
        ArgumentCaptor<ServletRequestWrapper> servletRequestWrapperCaptor = ArgumentCaptor.forClass(ServletRequestWrapper.class);
        ArgumentCaptor<String> processIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductionRequest> productionRequestCaptor = ArgumentCaptor.forClass(ProductionRequest.class);

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        String jobId = executeOperation.processAsync(mockExecuteRequest, "process1", mockWpsMetadata);

        PowerMockito.verifyNew(ExecuteRequestExtractor.class).withArguments(executeRequestCaptor.capture());
        PowerMockito.verifyNew(CalvalusFacade.class).withArguments(servletRequestWrapperCaptor.capture());
        PowerMockito.verifyNew(ProcessorNameParser.class).withArguments(processIdCaptor.capture());
        verify(mockCalvalusFacade).getProcessor(any(ProcessorNameParser.class));
        verify(mockCalvalusFacade).orderProductionAsynchronous(productionRequestCaptor.capture());

        assertThat(executeRequestCaptor.getValue(), equalTo(mockExecuteRequest));
        assertThat(servletRequestWrapperCaptor.getValue(), equalTo(mockServletRequestWrapper));
        assertThat(processIdCaptor.getValue(), equalTo("process1"));
        assertThat(productionRequestCaptor.getValue(), equalTo(mockProductionRequest));
        assertThat(jobId, equalTo("proces-00"));
    }

    @Test
    public void canCatchIOExceptionInProcessAsync() throws Exception {
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(mockServletRequestWrapper).thenThrow(new IOException("error"));

        thrownException.expect(WpsRuntimeException.class);
        thrownException.expectMessage("Unable to process the request asynchronously");

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        executeOperation.processAsync(mockExecuteRequest, "process1", mockWpsMetadata);
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
        ArgumentCaptor<WpsMetadata> wpsMetadataCaptor = ArgumentCaptor.forClass(WpsMetadata.class);

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        executeOperation.createAsyncExecuteResponse(mockExecuteRequest, mockWpsMetadata, true, "process1");

        verify(mockExecuteAcceptedResponse).getAcceptedWithLineageResponse(jobIdCaptor.capture(), dataInputsCaptor.capture(),
                                                                           output.capture(), wpsMetadataCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("process1"));
        assertThat(dataInputsCaptor.getValue(), equalTo(mockDataInputs));
        assertThat(output.getValue(), equalTo(mockOutputList));
        assertThat(wpsMetadataCaptor.getValue(), equalTo(mockWpsMetadata));
    }

    @Test
    public void canCreateNonLineageAsyncResponse() throws Exception {
        CalvalusExecuteResponseConverter mockExecuteAcceptedResponse = mock(CalvalusExecuteResponseConverter.class);
        PowerMockito.whenNew(CalvalusExecuteResponseConverter.class).withNoArguments().thenReturn(mockExecuteAcceptedResponse);

        ArgumentCaptor<String> jobIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<WpsMetadata> wpsMetadataCaptor = ArgumentCaptor.forClass(WpsMetadata.class);

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
        executeOperation.createAsyncExecuteResponse(mockExecuteRequest, mockWpsMetadata, false, "process1");

        verify(mockExecuteAcceptedResponse).getAcceptedResponse(jobIdCaptor.capture(), wpsMetadataCaptor.capture());

        assertThat(jobIdCaptor.getValue(), equalTo("process1"));
        assertThat(wpsMetadataCaptor.getValue(), equalTo(mockWpsMetadata));
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

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
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

        executeOperation = new CalvalusExecuteOperation(mockExecuteRequest, mockWpsMetadata, "process1");
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

    private void configureMockRequestUrl() {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        when(mockServletRequestWrapper.getRequestUrl()).thenReturn("http://dummyUrl.com");
        when(mockWpsMetadata.getServletRequestWrapper()).thenReturn(mockServletRequestWrapper);
    }
}