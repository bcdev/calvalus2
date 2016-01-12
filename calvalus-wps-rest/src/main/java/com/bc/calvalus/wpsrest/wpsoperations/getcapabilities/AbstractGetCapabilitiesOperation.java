package com.bc.calvalus.wpsrest.wpsoperations.getcapabilities;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.exception.ProcessesNotAvailableException;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.AbstractGetCapabilitiesResponseConverter;
import com.bc.calvalus.wpsrest.responses.CalvalusGetCapabilitiesResponseConverter;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;
import com.bc.calvalus.wpsrest.wpsoperations.WpsMetadata;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public abstract class AbstractGetCapabilitiesOperation {

    private Logger logger = getLogger();

    protected WpsMetadata wpsMetadata;

    public AbstractGetCapabilitiesOperation(WpsMetadata wpsMetadata) {
        this.wpsMetadata = wpsMetadata;
    }

    public String getCapabilities() {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            List<IWpsProcess> processes = getProcesses();

            AbstractGetCapabilitiesResponseConverter getCapabilitiesResponse = new CalvalusGetCapabilitiesResponseConverter();
            Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processes);

            jaxbHelper.marshal(capabilities, writer);
            return writer.toString();
        } catch (ProcessesNotAvailableException | JAXBException exception) {
            logger.log(Level.SEVERE, "Unable to create a response to a GetCapabilities request.", exception);
            ExceptionReport exceptionReport = getExceptionReport(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
                return writer.toString();
            } catch (JAXBException jaxbException) {
                logger.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
                return getWpsJaxbExceptionResponse();
            }
        }
    }

    public abstract Logger getLogger();

    public abstract List<IWpsProcess> getProcesses() throws ProcessesNotAvailableException;

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
