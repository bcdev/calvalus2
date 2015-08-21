package com.bc.calvalus.wps2;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProcessorExtractor;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProduction;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusProductionService;
import com.bc.calvalus.wps2.calvalusfacade.CalvalusStaging;
import com.bc.calvalus.wps2.jaxb.Capabilities;
import com.bc.calvalus.wps2.jaxb.ExceptionReport;
import com.bc.calvalus.wps2.jaxb.ExceptionType;
import com.bc.calvalus.wps2.jaxb.Execute;
import com.bc.calvalus.wps2.jaxb.ExecuteResponse;
import com.bc.calvalus.wps2.jaxb.ProcessDescriptions;
import com.bc.calvalus.wps2.responses.DescribeProcessResponse;
import com.bc.calvalus.wps2.responses.ExecuteSuccessfulResponse;
import com.bc.calvalus.wps2.responses.GetCapabilitiesResponse;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hans on 11/08/2015.
 */
public class ServletExample extends HttpServlet {

    private static final String WEBAPPS_ROOT = "/webapps/ROOT/";
    private static final String PORT_NUMBER = "9080";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
                throws IOException {
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        InputStream requestInputStream = request.getInputStream();
        processRequest(requestInputStream, out, request.getParameter("request"), request.getParameter("processor"));
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/xml");
        PrintWriter out = response.getWriter();
        InputStream requestInputStream = request.getInputStream();
        processRequest(requestInputStream, out, request.getParameter("request"), request.getParameter("processor"));
    }

    private void processRequest(InputStream requestInputStream, PrintWriter out, String requestType, String processorId) {
        try {
            switch (requestType) {
            case "GetCapabilities":
                createGetCapabilitiesResponse(out);
                break;
            case "DescribeProcess":
                createDescribeProcessResponse(out, processorId);
                break;
            case "Execute":
                processExecuteRequest(processorId, requestInputStream, out);
                break;
            default:
                out.println("Invalid request");
            }
        } catch (ProductionException | IOException | JAXBException | WpsException exception) {
            exception.printStackTrace();
            System.out.println("Error in processing the request : " + exception.getMessage());
            out.println("Error in processing the request : " + exception.getMessage());
            createExceptionResponse(out, exception);
        }
    }

    public void processExecuteRequest(String processorId, InputStream requestInputStream, PrintWriter out) {
        try {
            JaxbHelper jaxbHelper = new JaxbHelper();
            Execute execute = (Execute) jaxbHelper.unmarshal(requestInputStream);

            ExecuteRequestExtractor requestExtractor = new ExecuteRequestExtractor(execute);

            CalvalusConfig calvalusConfig = new CalvalusConfig();
            CalvalusProduction calvalusProduction = new CalvalusProduction();
            CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
            CalvalusStaging calvalusStaging = new CalvalusStaging();

            ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
            CalvalusProcessorExtractor calvalusProcessorExtractor = new CalvalusProcessorExtractor(productionService);
            ProcessorNameParser parser = new ProcessorNameParser(processorId);
            Processor processor = calvalusProcessorExtractor.getProcessor(parser);
            CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(requestExtractor, processor);

            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              getSystemUserName(),
                                                              calvalusDataInputs.getInputMapFormatted());


            Production production = calvalusProduction.orderProduction(productionService, request);
            calvalusStaging.stageProduction(productionService, production);

            if (productionService != null) {
                productionService.close();
            }

            String stagingDirectoryPath = calvalusConfig.getDefaultConfig().get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
            File stagingDirectory = new File((System.getProperty("catalina.base") + WEBAPPS_ROOT) + stagingDirectoryPath);
            File[] productResultFles = stagingDirectory.listFiles();
            List<String> productResultUrls = new ArrayList<>();
            if (productResultFles != null) {
                for (File productResultFile : productResultFles) {
                    String productUrl = "http://"
                                        + InetAddress.getLocalHost().getHostName()
                                        + ":" + PORT_NUMBER
                                        + "/" + stagingDirectoryPath
                                        + "/" + productResultFile.getName();
                    productResultUrls.add(productUrl);
                }
            }

            ExecuteSuccessfulResponse executeSuccessfulResponse = new ExecuteSuccessfulResponse();
            ExecuteResponse executeResponse = executeSuccessfulResponse.getExecuteResponse(productResultUrls);
            jaxbHelper.marshal(executeResponse, out);
        } catch (InterruptedException | IOException | JAXBException | DatatypeConfigurationException | ProductionException | WpsException exception) {
            createExceptionResponse(out, exception);
            exception.printStackTrace();
        }
    }

    private void createExceptionResponse(PrintWriter out, Exception exception) {
        ExceptionReport exceptionReport = new ExceptionReport();
        ExceptionType exceptionResponse = new ExceptionType();
        exceptionResponse.getExceptionText().add(exception.getMessage());
        exceptionResponse.setExceptionCode("exceptionCode");
        exceptionResponse.setLocator("locator");

        exceptionReport.getException().add(exceptionResponse);
        exceptionReport.setLang("Lang");
        exceptionReport.setVersion("version");
        JaxbHelper jaxbHelper = new JaxbHelper();
        try {
            jaxbHelper.marshal(exceptionReport, out);
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

    private void createDescribeProcessResponse(PrintWriter out, String processorId) throws ProductionException, IOException, JAXBException, WpsException {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);
        ProcessorNameParser parser = new ProcessorNameParser(processorId);
        Processor testProcessor = extractor.getProcessor(parser);

        DescribeProcessResponse describeProcessResponse = new DescribeProcessResponse();
        ProcessDescriptions processDescriptions = describeProcessResponse.getDescribeProcessResponse(testProcessor, extractor);

        JaxbHelper jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(processDescriptions, out);
    }

    private void createGetCapabilitiesResponse(PrintWriter out) throws ProductionException, IOException, JAXBException {
        CalvalusProductionService calvalusProductionService = new CalvalusProductionService();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        ProductionService productionService = calvalusProductionService.createProductionService(calvalusConfig);
        CalvalusProcessorExtractor extractor = new CalvalusProcessorExtractor(productionService);
        List<Processor> processors = extractor.getProcessors();

        GetCapabilitiesResponse getCapabilitiesResponse = new GetCapabilitiesResponse();
        Capabilities capabilities = getCapabilitiesResponse.createGetCapabilitiesResponse(processors);

        JaxbHelper jaxbHelper = new JaxbHelper();
        jaxbHelper.marshal(capabilities, out);
    }

    private String getSystemUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }
}
