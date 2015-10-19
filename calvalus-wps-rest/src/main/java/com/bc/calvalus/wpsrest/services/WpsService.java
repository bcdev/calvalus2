package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.exception.InvalidRequestException;
import com.bc.calvalus.wpsrest.exception.WpsInvalidParameterValueException;
import com.bc.calvalus.wpsrest.exception.WpsMissingParameterValueException;
import com.bc.calvalus.wpsrest.jaxb.CodeType;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * @author hans
 */
@Path("/")
public class WpsService {

    @GET
    @Produces(MediaType.APPLICATION_XML)
    public String getWpsService(@QueryParam("Service") String service,
                                @QueryParam("Request") String requestType,
                                @QueryParam("AcceptVersions") String acceptedVersion,
                                @QueryParam("Language") String language,
                                @QueryParam("Identifier") String processorId,
                                @QueryParam("Version") String version,
                                @QueryParam("JobId") String jobId,
                                @Context HttpServletRequest servletRequest) {

        ServletRequestWrapper servletRequestWrapper = new ServletRequestWrapper(servletRequest);

        if (StringUtils.isBlank(service)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Service");
            return stringWriter.toString();
        }
        if (StringUtils.isBlank(requestType)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Request");
            return stringWriter.toString();
        }
        if (!service.equals("WPS")) {
            StringWriter stringWriter = getInvalidParameterXmlWriter(service);
            return stringWriter.toString();
        }

        switch (requestType) {
        case "GetCapabilities":
            GetCapabilitiesService getCapabilitiesService = new GetCapabilitiesService();
            return getCapabilitiesService.getCapabilities(servletRequestWrapper);
        case "DescribeProcess":
            if (StringUtils.isBlank(version)) {
                StringWriter stringWriter = getMissingParameterXmlWriter("Version");
                return stringWriter.toString();
            }
            if (StringUtils.isBlank(processorId)) {
                StringWriter stringWriter = getMissingParameterXmlWriter("Identifier");
                return stringWriter.toString();
            }
            DescribeProcessService describeProcessService = new DescribeProcessService();
            return describeProcessService.describeProcess(servletRequestWrapper, processorId);
        case "GetStatus":
            if (StringUtils.isBlank(jobId)) {
                StringWriter stringWriter = getMissingParameterXmlWriter("JobId");
                return stringWriter.toString();
            }
            GetStatusService getStatusService = new GetStatusService();
            return getStatusService.getStatus(servletRequestWrapper, jobId);
        default:
            StringWriter stringWriter = getInvalidParameterXmlWriter("Request");
            return stringWriter.toString();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String postExecuteService(String request, @Context HttpServletRequest servletRequest) {
        Execute execute = getExecute(request);
        ServletRequestWrapper servletRequestWrapper = new ServletRequestWrapper(servletRequest);
        String service = execute.getService();
        String version = execute.getVersion();
        CodeType identifier = execute.getIdentifier();

        if (StringUtils.isBlank(service)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Service");
            return stringWriter.toString();
        }
        if (!"WPS".equals(service)) {
            StringWriter stringWriter = getInvalidParameterXmlWriter(service);
            return stringWriter.toString();
        }

        if (StringUtils.isBlank(version)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Version");
            return stringWriter.toString();
        }
        if (identifier == null || StringUtils.isBlank(identifier.getValue())) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Identifier");
            return stringWriter.toString();
        }

        String processorId = execute.getIdentifier().getValue();

        ExecuteService executeService = new ExecuteService();
        return executeService.execute(execute, servletRequestWrapper, processorId);
    }

    private Execute getExecute(String request) {
        InputStream requestInputStream = new ByteArrayInputStream(request.getBytes());
        JaxbHelper jaxbHelper = new JaxbHelper();
        Execute execute;
        try {
            execute = (Execute) jaxbHelper.unmarshal(requestInputStream);
        } catch (ClassCastException exception) {
            exception.printStackTrace();
            throw new InvalidRequestException("Invalid Execute request. Please see the WPS 1.0.0 guideline for the right Execute request structure.");
        } catch (JAXBException exception) {
            exception.printStackTrace();
            throw new InvalidRequestException("Invalid Execute request. "
                                              + (exception.getMessage() != null ? exception.getMessage() : exception.getCause().getMessage()));
        }
        return execute;
    }

    private StringWriter getMissingParameterXmlWriter(String missingParameter) {
        WpsMissingParameterValueException exception = new WpsMissingParameterValueException(missingParameter);
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        ExceptionReport exceptionReport = exceptionResponse.getMissingParameterExceptionResponse(exception, missingParameter);
        return getExceptionStringWriter(exceptionReport);
    }

    private StringWriter getInvalidParameterXmlWriter(String invalidParameter) {
        WpsInvalidParameterValueException exception = new WpsInvalidParameterValueException(invalidParameter);
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        ExceptionReport exceptionReport = exceptionResponse.getInvalidParameterExceptionResponse(exception, invalidParameter);
        return getExceptionStringWriter(exceptionReport);
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
