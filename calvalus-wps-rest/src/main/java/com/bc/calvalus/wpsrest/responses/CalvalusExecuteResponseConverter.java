package com.bc.calvalus.wpsrest.responses;

import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.*;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * @author hans
 */
public class CalvalusExecuteResponseConverter extends AbstractExecuteResponseConverter {

    public CalvalusExecuteResponseConverter() {
        super();
    }

    @Override
    public ExecuteResponse getAcceptedResponse(String jobId, WpsMetadata wpsMetadata) {
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);

        String getStatusUrl = getStatusUrl(jobId, servletRequestWrapper);
        executeResponse.setStatusLocation(getStatusUrl);

        return executeResponse;
    }

    @Override
    public ExecuteResponse getAcceptedWithLineageResponse(String jobId,
                                                          DataInputsType dataInputs,
                                                          List<DocumentOutputDefinitionType> rawDataOutput,
                                                          WpsMetadata wpsMetadata) {
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);
        statusType.setProcessAccepted("The request has been accepted. The status of the process can be found in the URL.");
        executeResponse.setStatus(statusType);
        String getStatusUrl = getStatusUrl(jobId, servletRequestWrapper);
        executeResponse.setStatusLocation(getStatusUrl);
        executeResponse.setDataInputs(dataInputs);
        OutputDefinitionsType outputDefinitionsType = new OutputDefinitionsType();
        outputDefinitionsType.getOutput().addAll(rawDataOutput);
        executeResponse.setOutputDefinitions(outputDefinitionsType);

        return executeResponse;
    }

    @Override
    public ExecuteResponse getSuccessfulResponse(List<String> resultUrls) {
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);
        statusType.setProcessSucceeded("The request has been processed successfully.");
        executeResponse.setStatus(statusType);

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
        executeResponse.setProcessOutputs(productUrl);

        return executeResponse;
    }

    @Override
    public ExecuteResponse getSuccessfulWithLineageResponse(List<String> resultUrls,
                                                            DataInputsType dataInputs,
                                                            List<DocumentOutputDefinitionType> outputType) {
        StatusType statusType = new StatusType();
        XMLGregorianCalendar currentTime = getXmlGregorianCalendar();
        statusType.setCreationTime(currentTime);
        statusType.setProcessSucceeded("The request has been processed successfully.");
        executeResponse.setStatus(statusType);

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
        executeResponse.setProcessOutputs(productUrl);
        executeResponse.setDataInputs(dataInputs);
        OutputDefinitionsType outputDefinitionsType = new OutputDefinitionsType();
        outputDefinitionsType.getOutput().addAll(outputType);
        executeResponse.setOutputDefinitions(outputDefinitionsType);

        return executeResponse;
    }

    @Override
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

    @Override
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

    private XMLGregorianCalendar getXmlGregorianCalendar() {
        GregorianCalendar gregorianCalendar = new GregorianCalendar();
        try {
            return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
        } catch (DatatypeConfigurationException exception) {
            throw new WpsException("Unable to create new Gregorian Calendar.", exception);
        }
    }

    private String getStatusUrl(String productId, ServletRequestWrapper servletRequestWrapper) {
        return servletRequestWrapper.getRequestUrl() + "?Service=WPS&Request=GetStatus&JobId=" + productId;
    }
}
