package com.bc.calvalus.wps.utils;


import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.exceptions.WpsRuntimeException;
import com.bc.wps.api.schema.ComplexDataType;
import com.bc.wps.api.schema.DataInputsType;
import com.bc.wps.api.schema.DataType;
import com.bc.wps.api.schema.DocumentOutputDefinitionType;
import com.bc.wps.api.schema.ExceptionReport;
import com.bc.wps.api.schema.ExceptionType;
import com.bc.wps.api.schema.ExecuteResponse;
import com.bc.wps.api.schema.OutputDataType;
import com.bc.wps.api.schema.OutputDefinitionsType;
import com.bc.wps.api.schema.OutputReferenceType;
import com.bc.wps.api.schema.ProcessFailedType;
import com.bc.wps.api.schema.ProcessStartedType;
import com.bc.wps.api.schema.StatusType;
import com.bc.wps.api.utils.WpsTypeConverter;
import com.bc.wps.utilities.PropertiesWrapper;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteResponseConverter {

    private ExecuteResponse executeResponse;

    public CalvalusExecuteResponseConverter() {
        this.executeResponse = new ExecuteResponse();
        this.executeResponse.setService("WPS");
        this.executeResponse.setVersion("1.0.0");
        this.executeResponse.setLang("en");
        this.executeResponse.setServiceInstance(PropertiesWrapper.get("wps.get.request.url"));
    }

    public ExecuteResponse getAcceptedResponse(String jobId, WpsServerContext context) {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);

        String getStatusUrl = getStatusUrl(jobId, context);
        executeResponse.setStatusLocation(getStatusUrl);

        return executeResponse;
    }

    public ExecuteResponse getAcceptedWithLineageResponse(String jobId,
                                                          DataInputsType dataInputs,
                                                          List<DocumentOutputDefinitionType> rawDataOutput,
                                                          WpsServerContext context) {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);
        String getStatusUrl = getStatusUrl(jobId, context);
        executeResponse.setStatusLocation(getStatusUrl);
        executeResponse.setDataInputs(dataInputs);
        OutputDefinitionsType outputDefinitionsType = new OutputDefinitionsType();
        outputDefinitionsType.getOutput().addAll(rawDataOutput);
        executeResponse.setOutputDefinitions(outputDefinitionsType);

        return executeResponse;
    }

    public ExecuteResponse getSuccessfulResponse(List<String> resultUrls, Date stopTime) {
        StatusType statusType = new StatusType();
        GregorianCalendar stopTimeGregorian = new GregorianCalendar();
        stopTimeGregorian.setTime(stopTime);
        XMLGregorianCalendar stopTimeXmlGregorian = getXmlGregorianCalendar(stopTimeGregorian);
        statusType.setCreationTime(stopTimeXmlGregorian);
        statusType.setProcessSucceeded("The request has been processed successfully.");
        executeResponse.setStatus(statusType);

        ExecuteResponse.ProcessOutputs productUrl = getProcessOutputs(resultUrls);
        executeResponse.setProcessOutputs(productUrl);

        return executeResponse;
    }

    public ExecuteResponse getSuccessfulWithLineageResponse(List<String> resultUrls,
                                                            DataInputsType dataInputs,
                                                            List<DocumentOutputDefinitionType> outputType) {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);
        statusType.setProcessSucceeded("The request has been processed successfully.");
        executeResponse.setStatus(statusType);

        ExecuteResponse.ProcessOutputs productUrl = getProcessOutputs(resultUrls);
        executeResponse.setProcessOutputs(productUrl);
        executeResponse.setDataInputs(dataInputs);
        OutputDefinitionsType outputDefinitionsType = new OutputDefinitionsType();
        outputDefinitionsType.getOutput().addAll(outputType);
        executeResponse.setOutputDefinitions(outputDefinitionsType);

        return executeResponse;
    }

    public ExecuteResponse getFailedResponse(String exceptionMessage) {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);

        ProcessFailedType processFailedType = new ProcessFailedType();
        ExceptionReport exceptionReport = new ExceptionReport();
        exceptionReport.setVersion("1.0.0");
        ExceptionType exceptionType = new ExceptionType();
        exceptionType.getExceptionText().add(exceptionMessage);
        exceptionType.setExceptionCode("NoApplicableCode");
        exceptionReport.getException().add(exceptionType);
        processFailedType.setExceptionReport(exceptionReport);
        statusType.setProcessFailed(processFailedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }

    public ExecuteResponse getStartedResponse(String state, float progress) {
        StatusType statusType = new StatusType();
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar(gregorianCalendar);
        statusType.setCreationTime(currentTime);

        ProcessStartedType processStartedType = new ProcessStartedType();
        processStartedType.setValue(state);
        processStartedType.setPercentCompleted(Math.round(progress));
        statusType.setProcessStarted(processStartedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }

    public ExecuteResponse getQuotationResponse(DataInputsType dataInputs) {
        StatusType statusType = new StatusType();
        GregorianCalendar stopTimeGregorian = new GregorianCalendar();
        stopTimeGregorian.setTime(new Date());
        XMLGregorianCalendar stopTimeXmlGregorian = getXmlGregorianCalendar(stopTimeGregorian);
        statusType.setCreationTime(stopTimeXmlGregorian);
        statusType.setProcessSucceeded("The request has been quoted successfully.");
        executeResponse.setStatus(statusType);

        ExecuteResponse.ProcessOutputs quoteJson = getQuoteProcessOutputs(dataInputs);
        executeResponse.setProcessOutputs(quoteJson);

        return executeResponse;
    }

    private ExecuteResponse.ProcessOutputs getQuoteProcessOutputs(DataInputsType dataInputs) {
        ExecuteResponse.ProcessOutputs processOutputs = new ExecuteResponse.ProcessOutputs();
        OutputDataType output = new OutputDataType();
        output.setIdentifier(WpsTypeConverter.str2CodeType("QUOTATION"));
        output.setTitle(WpsTypeConverter.str2LanguageStringType("Job Quotation"));
        DataType quoteData = new DataType();
        ComplexDataType quoteComplexData = new ComplexDataType();
        quoteComplexData.setMimeType("application/json");
        String quoteJsonString = getQuoteJsonString(dataInputs);
        quoteComplexData.getContent().add(quoteJsonString);
        quoteData.setComplexData(quoteComplexData);
        output.setData(quoteData);
        processOutputs.getOutput().add(output);
        return processOutputs;
    }

    private String getQuoteJsonString(DataInputsType dataInputs) {
        return "{\n" +
               "  \"id\" : \"t2cp_cluster5342_application_1479400262723_8995\",\n" +
               "  \"account\" : {\n" +
               "    \"platform\": \"urban-tep\",\n" +
               "    \"username\": \"emathot\",\n" +
               "    \"ref\": \"1738ad7b-534e-4aca-9861-b26fb9c0f983\"\n" +
               "  }\n" +
               "  \"compound\": {\n" +
               "    \"id\": \"t2cp_cluster5342_oozie_0004218-161117173256693-oozie-oozi-W\",\n" +
               "    \"name\": \"oozie:action:ID=0004218-161117173256693-oozie-oozi-W\"\n" +
               "    \"type\": \"WPS-OOZIE\"\n" +
               "    \"any\": {\n" +
               "      \"jobid\": \"oozie:action:T=map-reduce:W=t2-subset-snap:A=streaming-8247:ID=0004218-161117173256693-oozie-oozi-W\"\n" +
               "    }\n" +
               "  },\n" +
               "  \"quantity\" : [\n" +
               "    {\n" +
               "      \"id\": \"CPU_MILLISECONDS\",\n" +
               "      \"value\": 900000\n" +
               "    },\n" +
               "    {\n" +
               "      \"id\": \"PHYSICAL_MEMORY_BYTES\",\n" +
               "      \"value\": 2684354560\n" +
               "    },\n" +
               "    {\n" +
               "      \"id\": \"PROC_INSTANCE\",\n" +
               "      \"value\": 1\n" +
               "    },\n" +
               "    {\n" +
               "      \"id\": \"PROC_VOLUME_BYTES\",\n" +
               "      \"value\": 2097152\n" +
               "    }\n" +
               "  ]\n" +
               "  \"hostname\": \"cloud.terradue.com\",\n" +
               "  \"timestamp\": \"2017-01-10T10:32:16Z\",\n" +
               "  \"status\": \"QUOTATION\",\n" +
               "  \"location\": {\n" +
               "    \"coordinates\": [\n" +
               "      9.491,\n" +
               "      51.2993\n" +
               "    ],\n" +
               "  }\n" +
               "}";
    }

    private ExecuteResponse.ProcessOutputs getProcessOutputs(List<String> resultUrls) {
        ExecuteResponse.ProcessOutputs productUrl = new ExecuteResponse.ProcessOutputs();

        for (String productionResultUrl : resultUrls) {
            String identifier = "production_result";
            String title = "Production result";
            String abstractText = "This is the URL link to the production result";
            String mimeType = "application/octet-stream";
            if (productionResultUrl.endsWith("-metadata")) {
                identifier = "result_metadata";
                title = "Metadata OWS context XML";
                abstractText = "The URL to the result metadata file";
                mimeType = "application/atom+xml";
            }
            OutputDataType url = new OutputDataType();
            url.setIdentifier(WpsTypeConverter.str2CodeType(identifier));
            url.setTitle(WpsTypeConverter.str2LanguageStringType(title));
            url.setAbstract(WpsTypeConverter.str2LanguageStringType(abstractText));
            OutputReferenceType urlLink = new OutputReferenceType();
            urlLink.setHref(productionResultUrl);
            urlLink.setMimeType(mimeType);
            url.setReference(urlLink);

            productUrl.getOutput().add(url);
        }
        return productUrl;
    }

    private XMLGregorianCalendar getXmlGregorianCalendar(GregorianCalendar gregorianCalendar) {
        try {
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        } catch (DatatypeConfigurationException exception) {
            throw new WpsRuntimeException("Unable to create new Gregorian Calendar.", exception);
        }
    }

    private String getStatusUrl(String jobId, WpsServerContext context) {
        return context.getRequestUrl() + "?Service=WPS&Request=GetStatus&JobId=" + jobId;
    }
}
