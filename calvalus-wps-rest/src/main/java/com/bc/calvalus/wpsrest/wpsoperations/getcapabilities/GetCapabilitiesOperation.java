package com.bc.calvalus.wpsrest.wpsoperations.getcapabilities;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.AbstractGetCapabilitiesResponse;
import com.bc.calvalus.wpsrest.responses.CalvalusGetCapabilitiesResponse;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.WpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class GetCapabilitiesOperation {

    private Logger logger = getLogger();

    protected WpsMetadata wpsMetadata;

    public GetCapabilitiesOperation(WpsMetadata wpsMetadata) {
        this.wpsMetadata = wpsMetadata;
    }

    public String getCapabilities() {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            List<WpsProcess> processes = getProcesses();

            AbstractGetCapabilitiesResponse getCapabilitiesResponse = new CalvalusGetCapabilitiesResponse();
            Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processes);

            jaxbHelper.marshal(capabilities, writer);
            return writer.toString();
        } catch (WpsException | JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to create a response to a GetCapabilities request.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getGeneralExceptionResponse(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
                return getWpsJaxbExceptionResponse();
            }
            return writer.toString();
        }
    }

    private String getWpsJaxbExceptionResponse() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
               "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
               "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
               "    </Exception>\n" +
               "</ExceptionReport>\n";
    }

    public abstract Logger getLogger();

    public abstract List<WpsProcess> getProcesses();

}
