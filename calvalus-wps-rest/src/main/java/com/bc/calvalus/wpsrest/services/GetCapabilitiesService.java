package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.Capabilities;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import com.bc.calvalus.wpsrest.responses.GetCapabilitiesResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

/**
 * @author hans
 */
@Path("/GetCapabilities")
public class GetCapabilitiesService {

    @GET
    @Produces(MediaType.APPLICATION_XML)
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
            exception.printStackTrace();
            ExceptionResponse exceptionResponse = new ExceptionResponse();
            ExceptionReport exceptionReport = exceptionResponse.getGeneralExceptionResponse(exception);
            try {
                jaxbHelper.marshal(exceptionReport, writer);
            } catch (JAXBException e) {
                e.printStackTrace();
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
