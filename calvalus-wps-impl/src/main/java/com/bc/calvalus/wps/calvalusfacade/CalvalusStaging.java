package com.bc.calvalus.wps.calvalusfacade;


import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.ProductMetadataBuilder;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class handles all the product-staging-related operations.
 *
 * @author hans
 */
class CalvalusStaging {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String CALWPS_ROOT_PATH = PropertiesWrapper.get("wps.application.path");
    private static final String APP_NAME = PropertiesWrapper.get("wps.application.name");

    private final WpsServerContext wpsServerContext;

    CalvalusStaging(WpsServerContext wpsServerContext) {
        this.wpsServerContext = wpsServerContext;
    }

    void stageProduction(String jobid) throws WpsStagingException {
        LOG.info("Staging results...");
        try {
            ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            productionService.stageProductions(jobid);
        } catch (ProductionException | IOException exception) {
            throw new WpsStagingException(exception);
        }
    }

    List<String> getProductResultUrls(String jobId, Map<String, String> calvalusDefaultConfig) throws WpsResultProductException {
        Production production;
        try {
            ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            production = productionService.getProduction(jobId);
        } catch (ProductionException | IOException exception) {
            throw new WpsResultProductException(exception);
        }
        String stagingDirectoryPath = calvalusDefaultConfig.get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
        File stagingDirectory = new File(System.getProperty("catalina.base") + CALWPS_ROOT_PATH + "/" + stagingDirectoryPath);
        CalvalusLogger.getLogger().info("looking up results in staging dir " + stagingDirectory);

        Optional<File[]> productResultFiles = Optional.ofNullable(stagingDirectory.listFiles());
        if (productResultFiles.isPresent()) {
            return doGetProductResultUrls(production, stagingDirectoryPath, productResultFiles.get());
        } else {
            return new ArrayList<>();
        }
    }

    void observeStagingStatus(String jobId) throws WpsStagingException {
        ProductionService productionService;
        Production production;
        try {
            productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            production = productionService.getProduction(jobId);

            String userName = production.getProductionRequest().getUserName();
            while (!production.getStagingStatus().isDone()) {
                Thread.sleep(500);
                productionService.updateStatuses(userName);
                ProcessStatus stagingStatus = production.getStagingStatus();
                LOG.info(String.format("Staging status: state=%s, progress=%s, message='%s'",
                                       stagingStatus.getState(),
                                       stagingStatus.getProgress(),
                                       stagingStatus.getMessage()));
            }
        } catch (ProductionException | IOException | InterruptedException exception) {
            throw new WpsStagingException(exception);
        }

        if (production.getStagingStatus().getState() == ProcessState.COMPLETED) {
            LOG.info("Staging completed.");
        } else {
            LOG.log(Level.SEVERE, "Error: Staging did not complete normally: " + production.getStagingStatus().getMessage());
        }
    }

    void generateProductMetadata(String jobId, Map<String, String> calvalusDefaultConfig) throws ProductMetadataException {
        Production production;
        try {
            ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            production = productionService.getProduction(jobId);
        } catch (ProductionException | IOException exception) {
            throw new ProductMetadataException(exception);
        }
        String stagingDirectoryPath = calvalusDefaultConfig.get("calvalus.wps.staging.path") + "/" + production.getStagingPath();
        File stagingDirectory = new File(System.getProperty("catalina.base") + CALWPS_ROOT_PATH + "/" + stagingDirectoryPath);
        File[] productResultFiles = stagingDirectory.listFiles();
        if (productResultFiles == null) {
            throw new ProductMetadataException("No available product result files.");
        }
        File outputMetadata = new File(stagingDirectory, production.getName().replaceAll(" ", "_") + "-metadata");
        if (outputMetadata.exists()) {
            return;
        }

        try (PrintWriter out = new PrintWriter(outputMetadata.getAbsolutePath())) {
            ProductMetadata productMetadata = ProductMetadataBuilder.create()
                        .withProduction(production)
                        .withProductionResults(Arrays.asList(productResultFiles))
                        .withServerContext(wpsServerContext)
                        .build();

            VelocityWrapper velocityWrapper = new VelocityWrapper();
            String mergedMetadata = velocityWrapper.merge(productMetadata.getContextMap(), PropertiesWrapper.get("metadata.template"));
            out.println(mergedMetadata);
        } catch (ProductMetadataException exception) {
            LOG.log(Level.SEVERE, "Unable to create product metadata.", exception);
        } catch (FileNotFoundException exception) {
            LOG.log(Level.SEVERE, "Unable to write metadata file '" + outputMetadata + "'.", exception);
        }
    }

    private List<String> doGetProductResultUrls(Production production, String stagingDirectoryPath, File[] productResultFiles) {
        List<String> productResultUrls = new ArrayList<>();
        boolean withMetadataFile = false;
        for (File productResultFile : productResultFiles) {
            String productFileName = productResultFile.getName().toLowerCase();
            if (productFileName.endsWith("-metadata")/* || productFileName.endsWith(".csv")*/) {
                withMetadataFile = true;
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
        if (withMetadataFile) {
            UriBuilder builder = new UriBuilderImpl();
            String metadataUrl = builder.scheme("http")
                    .host(wpsServerContext.getHostAddress())
                    .port(wpsServerContext.getPort())
                    .path(APP_NAME)
                    .path(stagingDirectoryPath)
                    .path(production.getName().replaceAll(" ", "_") + "-metadata")
                    .build().toString();
            productResultUrls.add(metadataUrl);
        }
        return productResultUrls;
    }
}
