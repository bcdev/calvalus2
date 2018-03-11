package com.bc.calvalus.wps.wpsoperations;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusFacade;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.CalvalusExecuteResponseConverter;
import com.bc.calvalus.wps.utils.ExecuteRequestExtractor;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DataType;
import com.bc.wps.api.schema.Execute;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.InputType;
import com.bc.wps.api.schema.LiteralDataType;
import com.bc.wps.api.schema.ResponseDocumentType;
import com.bc.wps.api.schema.ResponseFormType;
import com.bc.wps.api.utils.WpsTypeConverter;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;
import org.junit.rules.*;
import org.junit.runner.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
            CalvalusExecuteOperation.class, CalvalusFacade.class, ExecuteRequestExtractor.class,
            ProcessorNameConverter.class, ProductionRequest.class, CalvalusDataInputs.class,
            CalvalusExecuteResponseConverter.class
})
public class CalvalusExecuteOperationTest {

    private static final String MOCK_PROCESSOR_ID = "processor-00";
    private CalvalusExecuteOperation executeOperation;

    private Execute mockExecuteRequest;
    private WpsRequestContext mockRequestContext;

    @Rule
    public ExpectedException thrownException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        mockExecuteRequest = mock(Execute.class);
        mockRequestContext = mock(WpsRequestContext.class);
        WpsServerContext mockServerContext = mock(WpsServerContext.class);
        when(mockServerContext.getHostAddress()).thenReturn("dummyHostName");
        when(mockServerContext.getRequestUrl()).thenReturn("http://dummyUrl.com/bc-wps/wps/provider1");
        when(mockServerContext.getPort()).thenReturn(8000);
        when(mockRequestContext.getServerContext()).thenReturn(mockServerContext);
    }

    @Test
    public void canExecuteAsync() throws Exception {
        ResponseFormType mockResponseForm = mock(ResponseFormType.class);
        ResponseDocumentType mockResponseDocument = mock(ResponseDocumentType.class);
        DataInputsType mockDataInputs = getDataInputs();
        when(mockResponseDocument.isStatus()).thenReturn(true);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getDataInputs()).thenReturn(mockDataInputs);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        when(mockExecuteRequest.getIdentifier()).thenReturn(WpsTypeConverter.str2CodeType("process1"));
        when(mockExecuteRequest.getDataInputs()).thenReturn(new DataInputsType());
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        when(mockStatus.getJobId()).thenReturn("process-00");
        when(mockProduction.getId()).thenReturn("process-00");
        when(mockCalvalusFacade.orderProductionAsynchronous(any(Execute.class))).thenReturn(mockStatus);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        executeOperation = new CalvalusExecuteOperation(MOCK_PROCESSOR_ID, mockRequestContext);
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
        DataInputsType mockDataInputs = getDataInputs();
        when(mockResponseDocument.isStatus()).thenReturn(false);
        when(mockResponseDocument.isLineage()).thenReturn(false);
        when(mockResponseForm.getResponseDocument()).thenReturn(mockResponseDocument);
        when(mockExecuteRequest.getDataInputs()).thenReturn(mockDataInputs);
        when(mockExecuteRequest.getResponseForm()).thenReturn(mockResponseForm);
        when(mockExecuteRequest.getIdentifier()).thenReturn(WpsTypeConverter.str2CodeType("process1"));
        when(mockExecuteRequest.getDataInputs()).thenReturn(new DataInputsType());
        configureProcessingMock();

        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Production mockProduction = mock(Production.class);
        List<String> resultUrlList = new ArrayList<>();
        resultUrlList.add("resultUrl1");
        resultUrlList.add("resultUrl2");
        LocalProductionStatus mockStatus = mock(LocalProductionStatus.class);
        when(mockStatus.getJobId()).thenReturn("process-00");
        when(mockStatus.getState()).thenReturn(ProcessState.COMPLETED.toString());
        when(mockStatus.getStopTime()).thenReturn(new Date(1451606400000L));
        when(mockStatus.getResultUrls()).thenReturn(resultUrlList);
        when(mockProduction.getId()).thenReturn("process-00");
        when(mockCalvalusFacade.orderProductionSynchronous(any(Execute.class))).thenReturn(mockStatus);
        when(mockCalvalusFacade.getProduction(anyString())).thenReturn(mockProduction);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        executeOperation = new CalvalusExecuteOperation(MOCK_PROCESSOR_ID, mockRequestContext);
        ExecuteResponse executeResponse = executeOperation.execute(mockExecuteRequest);

        assertThat(executeResponse.getStatus().getProcessSucceeded(), equalTo("The request has been processed successfully."));
        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("process1"));
        assertThat(executeResponse.getStatus().getCreationTime().toString(), equalTo("2016-01-01T00:00:00.000Z"));
        assertThat(executeResponse.getProcessOutputs().getOutput().size(), equalTo(2));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(), equalTo("resultUrl1"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(), equalTo("resultUrl2"));
    }

    @Test
    public void canReturnQuotation() throws Exception {
        DataInputsType mockDataInputs = getQuotationDataInputs();
        when(mockExecuteRequest.getDataInputs()).thenReturn(mockDataInputs);
        when(mockExecuteRequest.getIdentifier()).thenReturn(WpsTypeConverter.str2CodeType("process1"));
        configureProcessingMock();
        CalvalusFacade mockCalvalusFacade = mock(CalvalusFacade.class);
        Map<String, String> mockRequestHeaderMap = new HashMap<>();
        mockRequestHeaderMap.put("remoteUser", "user");
        mockRequestHeaderMap.put("remoteRef", "ref-1");
        when(mockCalvalusFacade.getRequestHeaderMap()).thenReturn(mockRequestHeaderMap);
        PowerMockito.whenNew(CalvalusFacade.class).withArguments(any(WpsRequestContext.class)).thenReturn(mockCalvalusFacade);

        executeOperation = new CalvalusExecuteOperation(MOCK_PROCESSOR_ID, mockRequestContext);
        ExecuteResponse executeResponse = executeOperation.execute(mockExecuteRequest);

        assertThat(executeResponse.getStatus().getProcessSucceeded(), equalTo("The request has been quoted successfully."));
        assertThat(executeResponse.getProcess().getIdentifier().getValue(), equalTo("process1"));
        assertThat(executeResponse.getProcessOutputs().getOutput().size(), equalTo(1));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getData().getComplexData().getContent().size(), equalTo(1));
        assertThat((String) executeResponse.getProcessOutputs().getOutput().get(0).getData().getComplexData().getContent().get(0),
                   containsString(
                               "{\n" +
                               "  \"id\": \"ref-1\",\n" +
                               "  \"account\": {\n" +
                               "    \"platform\": \"Brockmann Consult GmbH Processing Center\",\n" +
                               "    \"username\": \"user\",\n" +
                               "    \"ref\": \"ref-1\"\n" +
                               "  },\n  \"compound\": {\n" +
                               "    \"id\": \"any-id\",\n" +
                               "    \"name\": \"processName\",\n" +
                               "    \"type\": \"processType\"\n" +
                               "  },\n  \"quantity\": [\n" +
                               "    {\n" +
                               "      \"id\": \"CPU_MILLISECONDS\",\n" +
                               "      \"value\": 1\n" +
                               "    },\n" +
                               "    {\n" +
                               "      \"id\": \"PHYSICAL_MEMORY_BYTES\",\n" +
                               "      \"value\": 1\n" +
                               "    },\n" +
                               "    {\n" +
                               "      \"id\": \"PROC_VOLUME_BYTES\",\n" +
                               "      \"value\": 2\n" +
                               "    },\n" +
                               "    {\n" +
                               "      \"id\": \"PROC_INSTANCE\",\n" +
                               "      \"value\": 1\n" +
                               "    }\n" +
                               "  ],\n" +
                               "  \"hostName\": \"www.brockmann-consult.de\",\n"));
    }

    private DataInputsType getQuotationDataInputs() {
        return getDataInputsWithQuotation("true");
    }

    private DataInputsType getDataInputs() {
        return getDataInputsWithQuotation("false");
    }

    private DataInputsType getDataInputsWithQuotation(String isQuoatation) {
        DataInputsType mockDataInputs = new DataInputsType();
        InputType input1 = new InputType();
        input1.setIdentifier(WpsTypeConverter.str2CodeType("QUOTATION"));
        DataType quotationData = new DataType();
        LiteralDataType data = new LiteralDataType();
        data.setValue(isQuoatation);
        quotationData.setLiteralData(data);
        input1.setData(quotationData);
        mockDataInputs.getInput().add(input1);
        return mockDataInputs;
    }

    private ProductionRequest configureProcessingMock() throws Exception {
        ExecuteRequestExtractor mockRequestExtractor = mock(ExecuteRequestExtractor.class);
        PowerMockito.whenNew(ExecuteRequestExtractor.class).withArguments(any(Execute.class)).thenReturn(mockRequestExtractor);
        ProcessorNameConverter mockParser = mock(ProcessorNameConverter.class);
        PowerMockito.whenNew(ProcessorNameConverter.class).withArguments(anyString()).thenReturn(mockParser);
        CalvalusDataInputs mockCalvalusDataInputs = mock(CalvalusDataInputs.class);
        PowerMockito.whenNew(CalvalusDataInputs.class).withAnyArguments().thenReturn(mockCalvalusDataInputs);
        ProductionRequest mockProductionRequest = mock(ProductionRequest.class);
        PowerMockito.whenNew(ProductionRequest.class).withAnyArguments().thenReturn(mockProductionRequest);
        return mockProductionRequest;
    }

}
