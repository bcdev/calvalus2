package com.bc.calvalus.wpsrest.provider;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.exception.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;

/**
 * Created by hans on 08/10/2015.
 */
@Provider
public class WpsExceptionMapper implements ExceptionMapper<WpsException> {

    @Override
    public Response toResponse(WpsException exception) {
        exception.printStackTrace();
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
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return stringWriter;
    }
}
