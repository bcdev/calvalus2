package com.bc.calvalus.wpsrest.wpsoperations.execute;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.WpsMissingParameterValueException;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.jaxb.ResponseDocumentType;
import com.bc.calvalus.wpsrest.jaxb.ResponseFormType;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class AbstractExecuteOperation {

    private Logger logger = getLogger();

    public String execute(Execute executeRequest, WpsMetadata wpsMetadata, String processId) {
        StringWriter stringWriter = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        ServletRequestWrapper servletRequestWrapper = wpsMetadata.getServletRequestWrapper();
        try {
            ResponseFormType responseFormType = executeRequest.getResponseForm();
            ResponseDocumentType responseDocumentType = responseFormType.getResponseDocument();
            if (responseDocumentType.isStatus()) {
                String jobId = processAsync(executeRequest, processId, servletRequestWrapper);
                ExecuteResponse executeResponse = createAsyncExecuteResponse(executeRequest, servletRequestWrapper, responseDocumentType, jobId);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            } else {
                List<String> results = processSync(executeRequest, processId, servletRequestWrapper);
                ExecuteResponse executeResponse = createSyncExecuteResponse(executeRequest, responseDocumentType, results);
                jaxbHelper.marshal(executeResponse, stringWriter);
                return stringWriter.toString();
            }

        } catch (WpsMissingParameterValueException exception) {
            logger.log(Level.SEVERE, "A WpsMissingParameterValueException has been caught.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getMissingParameterExceptionResponse(exception, ""), stringWriter);
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
            }
            return stringWriter.toString();
        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException exception) {
            logger.log(Level.SEVERE, "Unable to process an Execute request.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            try {
                jaxbHelper.marshal(exceptionResponse.getGeneralExceptionResponse(exception), stringWriter);
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
            }
            return stringWriter.toString();
        }
    }

    public abstract List<String> processSync(Execute executeRequest, String processorId, ServletRequestWrapper servletRequestWrapper)
                throws IOException, ProductionException, JAXBException, InterruptedException;

    public abstract String processAsync(Execute executeRequest, String processorId, ServletRequestWrapper servletRequestWrapper)
                throws IOException, ProductionException, JAXBException;

    public abstract ExecuteResponse createAsyncExecuteResponse(Execute executeRequest, ServletRequestWrapper servletRequestWrapper,
                                                               ResponseDocumentType responseDocumentType, String productionId)
                throws DatatypeConfigurationException;

    public abstract ExecuteResponse createSyncExecuteResponse(Execute executeRequest, ResponseDocumentType responseDocumentType,
                                                              List<String> productResultUrls) throws DatatypeConfigurationException;

    public abstract Logger getLogger();

}
