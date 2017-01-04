package com.bc.calvalus.wps.calvalusfacade;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.api.WpsServerContext;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all the product-staging-related operations.
 *
 * @author hans
 */
public class CalvalusStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String CALWPS_ROOT_PATH = "/webapps/bc-wps/";
    private static final String APP_NAME = "bc-wps";

    private final WpsServerContext wpsServerContext;

    public CalvalusStaging(WpsServerContext wpsServerContext) {
        this.wpsServerContext = wpsServerContext;
    }

    protected void stageProduction(ProductionService productionService, Production production) throws ProductionException, IOException {
        logInfo("Staging results...");
        productionService.stageProductions(production.getId());
    }

    protected List<String> getProductResultUrls(Map<String, String> calvalusDefaultConfig, Production production)
                throws ProductionException, UnsupportedEncodingException {
        String stagingDirectoryPath = calvalusDefaultConfig.get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
        File stagingDirectory = new File((System.getProperty("catalina.base") + CALWPS_ROOT_PATH) + stagingDirectoryPath);

        File[] productResultFiles = stagingDirectory.listFiles();
        List<String> productResultUrls = new ArrayList<>();
        if (productResultFiles != null) {
            for (File productResultFile : productResultFiles) {
                String productFileName = productResultFile.getName().toLowerCase();
                if (productFileName.endsWith("-metadata") || productFileName.endsWith(".csv")) {
                    continue;
                }
                UriBuilder builder = new UriBuilderImpl();
                String productUrl = builder.scheme("http")
                            .host(wpsServerContext.getHostAddress())
                            .port(wpsServerContext.getPort())
                            .path(APP_NAME)
                            .path(stagingDirectoryPath)
                            .path(productResultFile.getName())
                            .build().toString();
                productResultUrls.add(productUrl);
            }
            UriBuilder builder = new UriBuilderImpl();
            String metadataUrl = builder.scheme("http")
                        .host(wpsServerContext.getHostAddress())
                        .port(wpsServerContext.getPort())
                        .path(APP_NAME)
                        .path(stagingDirectoryPath)
                        .path(production.getName() + "-metadata")
                        .build().toString();
            productResultUrls.add(metadataUrl);

            generateProductMetadata(production, stagingDirectory, productResultFiles);
        }
        return productResultUrls;
    }

    protected void observeStagingStatus(ProductionService productionService, Production production)
                throws InterruptedException {
        String userName = production.getProductionRequest().getUserName();
        while (!production.getStagingStatus().isDone()) {
            Thread.sleep(500);
            productionService.updateStatuses(userName);
            ProcessStatus stagingStatus = production.getStagingStatus();
            logInfo(String.format("Staging status: state=%s, progress=%s, message='%s'",
                                  stagingStatus.getState(),
                                  stagingStatus.getProgress(),
                                  stagingStatus.getMessage()));
        }
        if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
            logInfo("Staging completed.");
        } else {
            logError("Error: Staging did not complete normally: " + production.getStagingStatus().getMessage());
        }
    }

    private void generateProductMetadata(Production production, File stagingDirectory, File[] productResultFiles) throws ProductionException {
        File outputMetadata = new File(stagingDirectory, production.getName() + "-metadata");
        if (outputMetadata.exists()) {
            return;
        }

        ProductMetadata productMetadata = new ProductMetadata(production, Arrays.asList(productResultFiles), wpsServerContext);

        VelocityWrapper velocityWrapper = new VelocityWrapper();
        String mergedMetadata = velocityWrapper.merge(productMetadata.getContextMap(), "metadata-template.vm");

        try (PrintWriter out = new PrintWriter(outputMetadata.getAbsolutePath())) {
            out.println(mergedMetadata);
        } catch (FileNotFoundException exception) {
            LOG.log(Level.SEVERE, "Unable to write metadata file '" + outputMetadata + "'.", exception);
        }
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }

}
