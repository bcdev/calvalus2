package com.bc.calvalus.wpsrest.services;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.JaxbHelper;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.calvalusfacade.CalvalusHelper;
import com.bc.calvalus.wpsrest.jaxb.ExecuteResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteFailedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteStartedResponse;
import com.bc.calvalus.wpsrest.responses.ExecuteSuccessfulResponse;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import java.util.List;

/**
 * @author hans
 */
@Path("/Status")
public class GetStatusService {

    @GET
    @Path("{productionId}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStatus(ServletRequestWrapper servletRequestWrapper, @PathParam("productionId") String productionId) {
        JaxbHelper jaxbHelper = new JaxbHelper();
        StringWriter stringWriter = new StringWriter();
        try {
            CalvalusHelper calvalusHelper = new CalvalusHelper(servletRequestWrapper);
            ProductionService productionService = calvalusHelper.getProductionService();
            Production production = productionService.getProduction(productionId);

            ExecuteResponse executeResponse;
            if (isProductionJobFinishedAndSuccessful(production)) {
                List<String> productResultUrls = calvalusHelper.getProductResultUrls(production);
                ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
                executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
            } else if (isProductionJobFinishedAndFailed(production)) {
                ExecuteFailedResponse executeFailedResponse = new ExecuteFailedResponse();
                executeResponse = executeFailedResponse.getExecuteResponse(production.getProcessingStatus().getMessage());
            } else {
                ProcessStatus processingStatus = production.getProcessingStatus();
                ExecuteStartedResponse executeStartedResponse = new ExecuteStartedResponse();
                executeResponse = executeStartedResponse.getExecuteResponse(processingStatus.getState().toString(), 100 * processingStatus.getProgress());
            }
            jaxbHelper.marshal(executeResponse, stringWriter);
            return stringWriter.toString();

        } catch (ProductionException | IOException | DatatypeConfigurationException | JAXBException e) {
            e.printStackTrace();
            StringWriter exceptionStringWriter = new StringWriter();
            ExecuteFailedResponse executeFailedResponse = new ExecuteFailedResponse();
            try {
                ExecuteResponse executeResponse = executeFailedResponse.getExecuteResponse(e.getMessage());
                jaxbHelper.marshal(executeResponse, exceptionStringWriter);
            } catch (JAXBException | DatatypeConfigurationException exception) {
                exception.printStackTrace();
                return getDefaultExceptionResponse();
            }
            return exceptionStringWriter.toString();
        }
    }

    private boolean isProductionJobFinishedAndFailed(Production production) {
        return production.getProcessingStatus().getState().isDone();
    }

    private boolean isProductionJobFinishedAndSuccessful(Production production) {
        return production.getStagingStatus().getState() == ProcessState.COMPLETED;
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
