package com.bc.calvalus.wps.processes;

import com.bc.calvalus.wps.calvalusfacade.CalvalusProduction;
import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.wps.calvalusfacade.CalvalusConfig;
import com.bc.calvalus.wps.calvalusfacade.CalvalusDataInputs;
import com.bc.calvalus.wps.calvalusfacade.CalvalusStaging;
import com.bc.calvalus.wps.XmlResponseGenerator;
import com.bc.calvalus.wps.utility.CalvalusHelper;
import org.deegree.services.wps.Processlet;
import org.deegree.services.wps.ProcessletException;
import org.deegree.services.wps.ProcessletExecutionInfo;
import org.deegree.services.wps.ProcessletInputs;
import org.deegree.services.wps.ProcessletOutputs;
import org.deegree.services.wps.output.ComplexOutput;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hans on 21.07.2015.
 */
public class CalvalusProcesslet implements Processlet {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(),
                                                               "calvalus.config").getPath();
    private static final int PORT_NUMBER = 9080;
    private static final String WEBAPPS_ROOT = "/webapps/ROOT/";

    @Override
    public void process(ProcessletInputs processletInputs, ProcessletOutputs processletOutputs, ProcessletExecutionInfo info)
                throws ProcessletException {

        CalvalusDataInputs calvalusDataInputs = new CalvalusDataInputs(processletInputs);

        CalvalusHelper calvalusHelper = new CalvalusHelper();
        CalvalusStaging calvalusStaging = new CalvalusStaging();
        CalvalusConfig calvalusConfig = new CalvalusConfig();
        CalvalusProduction calvalusProduction = new CalvalusProduction();
        XmlResponseGenerator xmlResponseGenerator = new XmlResponseGenerator();

        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig(calvalusDataInputs);

        ProductionService productionService = null;
        try {
            logInfo(String.format("Loading Calvalus configuration '%s'...", DEFAULT_CONFIG_PATH));
            Map<String, String> config = ProductionServiceConfig.loadConfig(new File(DEFAULT_CONFIG_PATH), defaultConfig);
            logInfo("Configuration loaded.");
            info.setPercentCompleted(5);
            ProductionRequest request = new ProductionRequest(calvalusDataInputs.getValue("productionType"),
                                                              getSystemUserName(),
                                                              calvalusDataInputs.getInputMap());
            logInfo("Production request loaded, type is '" + request.getProductionType() + "'.");
            productionService = calvalusHelper.createProductionService(config);
            info.setPercentCompleted(10);

            Production production = calvalusProduction.orderProduction(productionService, request);
            if (production.isAutoStaging()) {
                calvalusStaging.stageProduction(productionService, production);
            }

            String stagingDirectoryPath = config.get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
            ComplexOutput secondOutput = (ComplexOutput) processletOutputs.getParameter("productionResults");
            XMLStreamWriter xmlStreamWriter = secondOutput.getXMLStreamWriter();
            File stagingDirectory = new File((System.getProperty("catalina.base") + WEBAPPS_ROOT) + stagingDirectoryPath);
            xmlResponseGenerator.constructXmlOutput(xmlStreamWriter, stagingDirectory, stagingDirectoryPath);
        } catch (InterruptedException exception) {
            logError("Error when ordering the product : " + exception.getMessage());
            throw new ProcessletException("Error when ordering the product : " + exception.getMessage());
        } catch (ProductionException exception) {
            logError("Error when creating productionService : " + exception.getMessage());
            throw new ProcessletException("Error when ordering the product : " + exception.getMessage());
        } catch (IOException | XMLStreamException exception) {
            logError("Error when loading calvalus configuration : " + exception.getMessage());
            throw new ProcessletException("Error when ordering the product : " + exception.getMessage());
        } finally {
            if (productionService != null) {
                try {
                    productionService.close();
                } catch (Exception e) {
                    logError("Warning: Failed to close production service! Job may still be alive!");
                }
            }
        }
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }

    private String getSystemUserName() {
        return System.getProperty("user.name", "anonymous").toLowerCase();
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }
}
