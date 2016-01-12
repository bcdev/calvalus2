package com.bc.calvalus.wpsrest.wpsoperations.execute;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.exception.FailedRequestException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.jaxb.ResponseFormType;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class AbstractExecuteOperation {

    private Logger logger = getLogger();

    protected Execute executeRequest;
    protected WpsMetadata wpsMetadata;
    protected String processId;

    public AbstractExecuteOperation(Execute executeRequest, WpsMetadata wpsMetadata, String processId) {
        this.executeRequest = executeRequest;
        this.wpsMetadata = wpsMetadata;
        this.processId = processId;
    }

    public String execute() {
        StringWriter stringWriter = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            ResponseFormType responseFormType = executeRequest.getResponseForm();
            ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
            boolean isAsynchronous = responseDocumentType.isStatus();
            boolean isLineage = responseDocumentType.isLineage();

            if (isAsynchronous) {
                String jobId = processAsync(executeRequest, processId, wpsMetadata);
                ExecuteResponse executeResponse = createAsyncExecuteResponse(executeRequest, wpsMetadata, isLineage, jobId);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else {
                List<String> results = processSync(executeRequest, processId, wpsMetadata);
                ExecuteResponse executeResponse = createSyncExecuteResponse(executeRequest, isLineage, results);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }
        } catch (FailedRequestException | JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to process an Execute request.", exception);
            ExceptionReport exceptionReport = getExceptionReport(exception);
            try {
                jaxbHelper.marshal(exceptionReport, stringWriter);
                return stringWriter.toString();
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
                return getWpsJaxbExceptionResponse();
            }
        }
    }

    public abstract List<String> processSync(Execute executeRequest, String processId, WpsMetadata wpsMetadata) throws FailedRequestException;

    public abstract String processAsync(Execute executeRequest, String processId, WpsMetadata wpsMetadata) throws FailedRequestException;

    public abstract ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, WpsMetadata wpsMetadata,
                                                               boolean isLineage, String jobId);

    public abstract ExecuteResponse createSyncExecuteResponse(Execute executeRequest, boolean isLineage,
                                                              List<String> productResultUrls);

    public abstract Logger getLogger();

    public ExceptionReport getExceptionReport(Exception exception) {
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        return exceptionResponse.getGeneralExceptionResponse(exception);
    }

    public String getWpsJaxbExceptionResponse() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
               "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
               "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
               "    </Exception>\n" +
               "</ExceptionReport>\n";
    }

}
