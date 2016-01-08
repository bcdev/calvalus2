package com.bc.calvalus.wpsrest.responses;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.DataInputsType;
import com.bc.calvalus.wpsrest.jaxb.DataType;
import com.bc.calvalus.wpsrest.jaxb.DocumentOutputDefinitionType;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.InputType;
import com.bc.calvalus.wpsrest.jaxb.LiteralDataType;
import com.bc.calvalus.wpsrest.jaxb.OutputDefinitionsType;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;
import org.junit.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteResponseTest {

    private CalvalusExecuteResponseConverter calvalusExecuteResponse;

    private WpsMetadata mockWpsMetadata;
    private ServletRequestWrapper mockServletRequestWrapper;

    @Before
    public void setUp() throws Exception {
        calvalusExecuteResponse = new CalvalusExecuteResponseConverter();
    }

    @Test
    public void canGetAcceptedResponse() throws Exception {
        configureMockRequestUrl();

        String jobId = "job-01";
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getAcceptedResponse(jobId, mockWpsMetadata);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessAccepted(), equalTo("The request has been accepted. The status of the process can be found in the URL."));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://dummyUrl.com?Service=WPS&Request=GetStatus&JobId=job-01"));
    }

    @Test
    public void canGetAcceptedResponseWithLineage() throws Exception {
        configureMockRequestUrl();
        DataInputsType dataInputs = new DataInputsType();
        InputType input1 = getInputType("inputDataSetName", "MERIS FSG v2013 L1b 2002-2012");
        dataInputs.getInput().add(input1);
        InputType input2 = getInputType("maxDate", "2009-06-03");
        dataInputs.getInput().add(input2);

        OutputDefinitionsType output = new OutputDefinitionsType();
        DocumentOutputDefinitionType outputType = getOutputType();
        output.getOutput().add(outputType);

        String jobId = "job-01";
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getAcceptedWithLineageResponse(jobId, dataInputs, output.getOutput(), mockWpsMetadata);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessAccepted(), equalTo("The request has been accepted. The status of the process can be found in the URL."));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://dummyUrl.com?Service=WPS&Request=GetStatus&JobId=job-01"));
        assertThat(executeResponse.getDataInputs().getInput().get(0).getIdentifier().getValue(), equalTo("inputDataSetName"));
        assertThat(executeResponse.getDataInputs().getInput().get(0).getData().getLiteralData().getValue(), equalTo("MERIS FSG v2013 L1b 2002-2012"));
        assertThat(executeResponse.getDataInputs().getInput().get(1).getIdentifier().getValue(), equalTo("maxDate"));
        assertThat(executeResponse.getDataInputs().getInput().get(1).getData().getLiteralData().getValue(), equalTo("2009-06-03"));
        assertThat(executeResponse.getOutputDefinitions().getOutput().get(0).getIdentifier().getValue(), equalTo("productionResults"));
    }

    @Test
    public void canGetSuccessfulResponse() throws Exception {
        List<String> mockResultUrlList = new ArrayList<>();
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc");
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip");
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getSuccessfulResponse(mockResultUrlList);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));

    }

    @Test
    public void canGetSuccessfulResponseWithLineage() throws Exception {
        DataInputsType dataInputs = new DataInputsType();
        InputType input1 = getInputType("inputDataSetName", "MERIS FSG v2013 L1b 2002-2012");
        dataInputs.getInput().add(input1);
        InputType input2 = getInputType("maxDate", "2009-06-03");
        dataInputs.getInput().add(input2);

        OutputDefinitionsType output = new OutputDefinitionsType();
        DocumentOutputDefinitionType outputType = getOutputType();
        output.getOutput().add(outputType);

        List<String> mockResultUrlList = new ArrayList<>();
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc");
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip");
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getSuccessfulWithLineageResponse(mockResultUrlList, dataInputs, output.getOutput());

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getDataInputs().getInput().get(0).getIdentifier().getValue(), equalTo("inputDataSetName"));
        assertThat(executeResponse.getDataInputs().getInput().get(0).getData().getLiteralData().getValue(), equalTo("MERIS FSG v2013 L1b 2002-2012"));
        assertThat(executeResponse.getDataInputs().getInput().get(1).getIdentifier().getValue(), equalTo("maxDate"));
        assertThat(executeResponse.getDataInputs().getInput().get(1).getData().getLiteralData().getValue(), equalTo("2009-06-03"));
        assertThat(executeResponse.getOutputDefinitions().getOutput().get(0).getIdentifier().getValue(), equalTo("productionResults"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("productionResults"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));

    }

    @Test
    public void canGetFailedResponse() throws Exception {
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getFailedResponse("Unable to process the request");

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessFailed().getExceptionReport().getVersion(), equalTo("1"));
        assertThat(executeResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionText().get(0),
                   equalTo("Unable to process the request"));

    }

    @Test
    public void canGetStartedResponse() throws Exception {
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getStartedResponse("RUNNING", 50);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessStarted().getValue(), equalTo("RUNNING"));
        assertThat(executeResponse.getStatus().getProcessStarted().getPercentCompleted(), equalTo(50));

    }

    private DocumentOutputDefinitionType getOutputType() {
        DocumentOutputDefinitionType outputType = new DocumentOutputDefinitionType();
        CodeType outputId = new CodeType();
        outputId.setValue("productionResults");
        outputType.setIdentifier(outputId);
        return outputType;
    }

    private InputType getInputType(String parameterId, String value) {
        InputType input1 = new InputType();
        CodeType input1Id = new CodeType();
        input1Id.setValue(parameterId);
        input1.setIdentifier(input1Id);
        DataType input1Data = new DataType();
        LiteralDataType input1LiteralData = new LiteralDataType();
        input1LiteralData.setValue(value);
        input1Data.setLiteralData(input1LiteralData);
        input1.setData(input1Data);
        return input1;
    }

    private void configureMockRequestUrl() {
        mockWpsMetadata = mock(WpsMetadata.class);
        mockServletRequestWrapper = mock(ServletRequestWrapper.class);
        when(mockServletRequestWrapper.getRequestUrl()).thenReturn("http://dummyUrl.com");
        when(mockWpsMetadata.getServletRequestWrapper()).thenReturn(mockServletRequestWrapper);
    }
}