package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.GetCapabilitiesResponse;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all the GetCapabilities requests.
 *
 * @author hans
 */
public class GetCapabilitiesService {

    private static final Logger LOG = CalvalusLogger.getLogger();

    public String getCapabilities(ServletRequestWrapper servletRequestWrapper) {
        StringWriter writer = new StringWriter();
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);

            List<Processor> processors = calvalusHelper.getProcessors();

            GetCapabilitiesResponse getCapabilitiesResponse = new GetCapabilitiesResponse();
            Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processors);

            jaxbHelper.marshal(capabilities, writer);
            return writer.toString();
        } catch (ProductionException | IOException | JAXBException exception) {
            LOG.log(Level.SEVERE, "Unable to create a response to a GetCapabilities request.", exception);
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getGeneralExceptionResponse(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException jaxbException) {
                LOG.log(Level.SEVERE, "Unable to marshal the WPS exception.", jaxbException);
                return getDefaultExceptionResponse();
            }
            return writer.toString();
        }
    }

    private String getDefaultExceptionResponse() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
               "<ExceptionReport version=\"version\" xml:lang=\"Lang\">\n" +
               "    <Exception exceptionCode=\"NoApplicableCode\">\n" +
               "        <ExceptionText>Unable to generate the exception XML : JAXB Exception.</ExceptionText>\n" +
               "    </Exception>\n" +
               "</ExceptionReport>\n";
    }
}
