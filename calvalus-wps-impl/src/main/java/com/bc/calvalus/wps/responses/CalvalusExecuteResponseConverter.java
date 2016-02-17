package com.bc.calvalus.wps.responses;


import com.bc.calvalus.wps.exceptions.WpsRuntimeException;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.api.schema.CodeType;
import com.bc.wps.api.schema.DataInputsType;
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

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
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
    }

    public ExecuteResponse getAcceptedResponse(String jobId, WpsServerContext context) {
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
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
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
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

    public ExecuteResponse getSuccessfulResponse(List<String> resultUrls) {
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);
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
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
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
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);

        ProcessFailedType processFailedType = new ProcessFailedType();
        ExceptionReport exceptionReport = new ExceptionReport();
        exceptionReport.setVersion("1");
        ExceptionType exceptionType = new ExceptionType();
        exceptionType.getExceptionText().add(exceptionMessage);
        exceptionReport.getException().add(exceptionType);
        processFailedType.setExceptionReport(exceptionReport);
        statusType.setProcessFailed(processFailedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }

    public ExecuteResponse getStartedResponse(String state, float progress) {
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);

        ProcessStartedType processStartedType = new ProcessStartedType();
        processStartedType.setValue(state);
        processStartedType.setPercentCompleted(Math.round(progress));
        statusType.setProcessStarted(processStartedType);
        executeResponse.setStatus(statusType);

        return executeResponse;
    }

    private ExecuteResponse.ProcessOutputs getProcessOutputs(List<String> resultUrls) {
        ExecuteResponse.ProcessOutputs productUrl = new ExecuteResponse.ProcessOutputs();

        for (String productionResultUrl : resultUrls) {
            OutputDataType url = new OutputDataType();
            CodeType outputId = new CodeType();
            outputId.setValue("productionResults");
            url.setIdentifier(outputId);
            OutputReferenceType urlLink = new OutputReferenceType();
            urlLink.setHref(productionResultUrl);
            urlLink.setMimeType("binary");
            url.setReference(urlLink);

            productUrl.getOutput().add(url);
        }
        return productUrl;
    }

    private XMLGregorianCalendar getXmlGregorianCalendar() {
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        try {
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        } catch (DatatypeConfigurationException exception) {
            throw new WpsRuntimeException("Unable to create new Gregorian Calendar.", exception);
        }
    }

    private String getStatusUrl(String productId, WpsServerContext context) {
        return context.getRequestUrl() + "?Service=WPS&Request=GetStatus&JobId=" + productId;
    }
}
