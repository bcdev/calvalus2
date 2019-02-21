package com.bc.calvalus.wps.utils;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.CodeType;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DataType;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.InputType;
import com.bc.wps.api.schema.LiteralDataType;
import com.bc.wps.api.schema.OutputDefinitionsType;
import com.bc.wps.utilities.PropertiesWrapper;
import org.junit.*;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteResponseConverterTest {

    private CalvalusExecuteResponseConverter calvalusExecuteResponse;

    private WpsServerContext mockServerContext;

    @Before
    public void setUp() throws Exception {
        PropertiesWrapper.loadConfigFile("calvalus-wps-test.properties");
        calvalusExecuteResponse = new CalvalusExecuteResponseConverter();
        mockServerContext = mock(WpsServerContext.class);
        when(mockServerContext.getHostAddress()).thenReturn("dummyUrl.com");
        when(mockServerContext.getRequestUrl()).thenReturn("http://dummyUrl.com/bc-wps/wps/provider1");
    }

    @Test
    public void canGetAcceptedResponse() throws Exception {
        String jobId = "job-01";
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getAcceptedResponse(jobId, mockServerContext);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessAccepted(), equalTo("The request has been accepted. In order to check the status, use GetStatus with JobId 'job-01'."));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://dummyUrl.com/bc-wps/wps/provider1?Service=WPS&Request=GetStatus&JobId=job-01"));
    }

    @Test
    public void canGetAcceptedResponseWithLineage() throws Exception {
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

        ExecuteResponse executeResponse = calvalusExecuteResponse.getAcceptedWithLineageResponse(jobId, dataInputs, output.getOutput(), mockServerContext);

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getStatus().getProcessAccepted(), equalTo("The request has been accepted. The status of the process can be found in the URL."));
        assertThat(executeResponse.getStatusLocation(), equalTo("http://dummyUrl.com/bc-wps/wps/provider1?Service=WPS&Request=GetStatus&JobId=job-01"));
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
        mockResultUrlList.add("http://www.dummy.com/wps/staging/user//123546_L3_123456/zzz-metadata");
        Calendar calendar = Calendar.getInstance();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getSuccessfulResponse(mockResultUrlList, new Date());

        assertThat(executeResponse.getStatus().getCreationTime().getDay(), equalTo(calendar.get(Calendar.DAY_OF_MONTH)));
        assertThat(executeResponse.getStatus().getCreationTime().getMonth(), equalTo(calendar.get(Calendar.MONTH) + 1)); // +1 because month starts from 0
        assertThat(executeResponse.getStatus().getCreationTime().getYear(), equalTo(calendar.get(Calendar.YEAR)));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(), equalTo("production_result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getTitle().getValue(), equalTo("Production result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getAbstract().getValue(),
                   equalTo("This is the URL link to the production result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(), equalTo("production_result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getTitle().getValue(), equalTo("Production result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getAbstract().getValue(),
                   equalTo("This is the URL link to the production result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/yyy.zip"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(2).getIdentifier().getValue(), equalTo("result_metadata"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(2).getTitle().getValue(), equalTo("Metadata OWS context XML"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(2).getAbstract().getValue(),
                   equalTo("The URL to the result metadata file"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(2).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/zzz-metadata"));

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
                   equalTo("production_result"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getReference().getHref(),
                   equalTo("http://www.dummy.com/wps/staging/user//123546_L3_123456/xxx.nc"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(1).getIdentifier().getValue(),
                   equalTo("production_result"));
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
        assertThat(executeResponse.getStatus().getProcessFailed().getExceptionReport().getVersion(), equalTo("1.0.0"));
        assertThat(executeResponse.getStatus().getProcessFailed().getExceptionReport().getException().get(0).getExceptionCode(),
                   equalTo("NoApplicableCode"));
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

    @Test
    public void canGetQuotationResponse() throws Exception {
        DataInputsType dataInputs = getDataInputsType();

        ExecuteResponse executeResponse = calvalusExecuteResponse.getQuotationResponse("tepUser", "ref-1", dataInputs);

        assertThat(executeResponse.getStatus().getProcessSucceeded(), equalTo("The request has been quoted successfully."));
        assertThat(executeResponse.getProcessOutputs().getOutput().size(), equalTo(1));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getIdentifier().getValue(), equalTo("QUOTATION"));
        assertThat(executeResponse.getProcessOutputs().getOutput().get(0).getTitle().getValue(), equalTo("Job Quotation"));
        assertThat((String) executeResponse.getProcessOutputs().getOutput().get(0).getData().getComplexData().getContent().get(0),
                   containsString("{\n" +
                                  "  \"id\": \"ref-1\",\n" +
                                  "  \"account\": {\n" +
                                  "    \"platform\": \"Brockmann Consult GmbH Processing Center\",\n" +
                                  "    \"username\": \"tepUser\",\n" +
                                  "    \"ref\": \"ref-1\"\n" +
                                  "  },\n" +
                                  "  \"compound\": {\n" +
                                  "    \"id\": \"any-id\",\n" +
                                  "    \"name\": \"processName\",\n" +
                                  "    \"type\": \"processType\"\n" +
                                  "  },\n" +
                                  "  \"quantity\": [\n" +
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
        assertThat((String) executeResponse.getProcessOutputs().getOutput().get(0).getData().getComplexData().getContent().get(0),
                   containsString("  \"status\": \"QUOTATION\"\n" +
                                  "}"));
    }

    private DataInputsType getDataInputsType() {
        DataInputsType dataInputs = new DataInputsType();
        InputType input1 = getInputType("inputDataSetName", "MERIS FSG v2013 L1b 2002-2012");
        dataInputs.getInput().add(input1);
        InputType input2 = getInputType("maxDate", "2009-06-03");
        dataInputs.getInput().add(input2);
        return dataInputs;
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
}