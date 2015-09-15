package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.WpsException;
import com.bc.calvalus.wpsrest.jaxb.ExceptionReport;
import com.bc.calvalus.wpsrest.jaxb.Execute;
import com.bc.calvalus.wpsrest.responses.ExceptionResponse;
import org.apache.commons.lang.StringUtils;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import javax.xml.bind.JAXBException;
import java.io.StringWriter;

/**
 * Created by hans on 03/09/2015.
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
                                @Context SecurityContext security) {

        String userName = security.getUserPrincipal().getName();

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
            return getCapabilitiesService.getCapabilities(userName);
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
            return describeProcessService.describeProcess(userName, processorId);
        case "GetStatus":
            GetStatusService getStatusService = new GetStatusService();
            return getStatusService.getStatus(userName, jobId);
        default:
            StringWriter stringWriter = getInvalidParameterXmlWriter("Request");
            return stringWriter.toString();
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    public String postExecuteService(Execute execute, @Context SecurityContext security) {

        String service = execute.getService();
        String version = execute.getVersion();
        String processorId = execute.getIdentifier().getValue();
        String userName = security.getUserPrincipal().getName();
        System.out.println("userName = " + userName);

        if (StringUtils.isBlank(service)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Service");
            return stringWriter.toString();
        }
        if (!service.equals("WPS")) {
            StringWriter stringWriter = getInvalidParameterXmlWriter(service);
            return stringWriter.toString();
        }

        if (StringUtils.isBlank(version)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Version");
            return stringWriter.toString();
        }
        if (StringUtils.isBlank(processorId)) {
            StringWriter stringWriter = getMissingParameterXmlWriter("Identifier");
            return stringWriter.toString();
        }

        ExecuteService executeService = new ExecuteService();
        return executeService.execute(execute, userName, processorId);

    }

    private StringWriter getMissingParameterXmlWriter(String missingParameter) {
        WpsException exception = new WpsException("Missing parameter '" + missingParameter + "'.");
        ExceptionResponse exceptionResponse = new ExceptionResponse();
        ExceptionReport exceptionReport = exceptionResponse.getMissingParameterExceptionResponse(exception, missingParameter);
        return getExceptionStringWriter(exceptionReport);
    }

    private StringWriter getInvalidParameterXmlWriter(String invalidParameter) {
        WpsException exception = new WpsException("Invalid parameter value '" + invalidParameter + "'.");
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
