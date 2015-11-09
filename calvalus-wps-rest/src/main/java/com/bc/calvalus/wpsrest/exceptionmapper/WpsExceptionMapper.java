package com.bc.calvalus.wpsrest.exceptionmapper;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class maps any unhandled WpsException to a proper WPS Exception response.
 * <p/>
 * Created by hans on 08/10/2015.
 */
@Provider
public class WpsExceptionMapper implements ExceptionMapper<WpsException> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public Response toResponse(WpsException exception) {
        LOG.log(Level.SEVERE, "A RunTimeException has been caught.", exception);
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        StringWriter stringWriter = getExceptionStringWriter(exceptionResponse.getGeneralExceptionResponse(exception));
        return Response.serverError()
                    .entity(stringWriter.toString())
                    .build();
    }

    private StringWriter getExceptionStringWriter(ExceptionReport exceptionReport) {
        JaxbHelper jaxbHelper = new JaxbHelper();
        StringWriter stringWriter = new StringWriter();
        try {
            jaxbHelper.marshal(exceptionReport, stringWriter);
        } catch (JAXBException exception) {
            LOG.log(Level.SEVERE, "Unable to marshal the WPS Exception.", exception);
        }
        return stringWriter;
    }
}
