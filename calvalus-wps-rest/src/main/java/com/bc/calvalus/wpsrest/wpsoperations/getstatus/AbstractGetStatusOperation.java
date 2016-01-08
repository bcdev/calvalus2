package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class AbstractGetStatusOperation {

    private Logger logger = getLogger();
    protected WpsMetadata wpsMetadata;
    protected String jobId;

    public AbstractGetStatusOperation(WpsMetadata wpsMetadata, String jobId) {
        this.wpsMetadata = wpsMetadata;
        this.jobId = jobId;
    }

    public String getStatus() {
        JaxbHelper jaxbHelper = new JaxbHelper();
        StringWriter stringWriter = new StringWriter();
        try {
            ExecuteResponse executeResponse = getExecuteResponse();
            jaxbHelper.marshal(executeResponse, stringWriter);
            return stringWriter.toString();

        } catch (JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to create a response to a GetCapabilities request.", exception);
            StringWriter exceptionStringWriter = new StringWriter();
            ExceptionReport exceptionReport = getExceptionReport(exception);
            try {
                jaxbHelper.marshal(exceptionReport, exceptionStringWriter);
            } catch (JAXBException marshallingException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", marshallingException);
                return getWpsJaxbExceptionResponse();
            }
            return exceptionStringWriter.toString();
        }
    }

    public abstract ExecuteResponse getExecuteAcceptedResponse();

    public abstract ExecuteResponse getExecuteFailedResponse();

    public abstract ExecuteResponse getExecuteSuccessfulResponse();

    public abstract boolean isProductionJobFinishedAndSuccessful();

    public abstract boolean isProductionJobFinishedAndFailed();

    public abstract Logger getLogger();

    private ExecuteResponse getExecuteResponse() {
        ExecuteResponse executeResponse;
        if (isProductionJobFinishedAndSuccessful()) {
            executeResponse = getExecuteSuccessfulResponse();
        } else if (isProductionJobFinishedAndFailed()) {
            executeResponse = getExecuteFailedResponse();
        } else {
            executeResponse = getExecuteAcceptedResponse();
        }
        return executeResponse;
    }

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
