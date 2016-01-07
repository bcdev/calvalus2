package com.bc.calvalus.wpsrest.wpsoperations.getstatus;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteFailedResponse;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
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
            ExecuteFailedResponse executeFailedResponse = new ExecuteFailedResponse();
            try {
                ExecuteResponse executeResponse = executeFailedResponse.getExecuteResponse(exception.getMessage());
                jaxbHelper.marshal(executeResponse, exceptionStringWriter);
            } catch (JAXBException | DatatypeConfigurationException marshallingException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", marshallingException);
                return getDefaultExceptionResponse();
            }
            return exceptionStringWriter.toString();
        }
    }

    public abstract ExecuteResponse getExecuteAcceptedResponse();

    public abstract ExecuteResponse getExecuteFailedResponse();

    public abstract ExecuteResponse getExecuteSuccessfulResponse();

    public abstract boolean isProductionJobFinishedAndFailed();

    public abstract boolean isProductionJobFinishedAndSuccessful();

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

    private String getDefaultExceptionResponse() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ExecuteResponse service=\"WPS\" version=\"1.0.0\" xml:lang=\"en\" xmlns:ns5=\"http://www.brockmann-consult.de/calwps/calwpsL3Parameters-schema.xsd\" xmlns:ns1=\"http://www.opengis.net/ows/1.1\" xmlns:ns4=\"http://www.opengis.net/wps/1.0.0\" xmlns:ns3=\"http://www.w3.org/1999/xlink\">\n" +
               "    <Status creationTime=\"" + new Date() + "\"" +
               "        <ProcessFailed>\n" +
               "            <ns1:ExceptionReport version=\"1\">\n" +
               "                <Exception>\n" +
               "                    <ExceptionText>Unable to generate the requested status : JAXB Exception.</ExceptionText>\n" +
               "                </Exception>\n" +
               "            </ns1:ExceptionReport>\n" +
               "        </ProcessFailed>\n" +
               "    </Status>\n" +
               "</ExecuteResponse>";
    }

}
