package com.bc.calvalus.wpsrest.exceptionmapper;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import org.apache.commons.lang.StringUtils;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class maps any NullPointerException to a proper WPS Exception response.
 * <p/>
 * Created by hans on 08/10/2015.
 */
@Provider
public class NullPointerExceptionMapper implements ExceptionMapper<NullPointerException> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public Response toResponse(NullPointerException exception) {
        LOG.log(Level.SEVERE, "A NullPointerException has been caught.", exception);
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        StringWriter stringWriter = getExceptionStringWriter(exceptionResponse.
                    getGeneralExceptionWithCustomMessageResponse("A value is missing" +
                                                                 (StringUtils.isNotBlank(exception.getMessage()) ? " : " + exception.getMessage() : "")));
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
