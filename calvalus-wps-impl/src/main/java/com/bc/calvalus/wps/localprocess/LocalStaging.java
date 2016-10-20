package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.InvalidProcessorIdException;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.ProductMetadataBuilder;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.utilities.PropertiesWrapper;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author hans
 */
class LocalStaging {

    private static final String CATALINA_BASE = System.getProperty("catalina.base");

    List<String> getProductUrls(String jobId, String systemUserName, String remoteUserName, String hostName, int portNumber) {
        Path targetDirectoryPath;
        if (systemUserName.equalsIgnoreCase(remoteUserName)) {
            targetDirectoryPath = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"),
                                            PropertiesWrapper.get("utep.output.directory"), systemUserName, jobId);
        } else {
            targetDirectoryPath = Paths.get(CATALINA_BASE + PropertiesWrapper.get("wps.application.path"),
                                            PropertiesWrapper.get("utep.output.directory"), systemUserName, remoteUserName, jobId);
        }
        File targetDir = targetDirectoryPath.toFile();
        List<String> resultUrls = new ArrayList<>();
        File[] resultProductFiles = targetDir.listFiles();
        if (resultProductFiles != null) {
            for (File resultProductFile : resultProductFiles) {
                UriBuilder builder = new UriBuilderImpl();
                String productUrl = builder.scheme("http")
                            .host(hostName)
                            .port(portNumber)
                            .path(PropertiesWrapper.get("wps.application.name"))
                            .path(PropertiesWrapper.get("utep.output.directory"))
                            .path(systemUserName)
                            .path(systemUserName.equals(remoteUserName) ? "/" : remoteUserName)
                            .path(targetDir.getName())
                            .path(resultProductFile.getName()).build().toString();
                resultUrls.add(productUrl);
            }

            UriBuilder builder = new UriBuilderImpl();
            String metadataUrl = builder.scheme("http")
                        .host(hostName)
                        .port(portNumber)
                        .path(PropertiesWrapper.get("wps.application.name"))
                        .path(PropertiesWrapper.get("utep.output.directory"))
                        .path(systemUserName)
                        .path(systemUserName.equals(remoteUserName) ? "/" : remoteUserName)
                        .path(targetDir.getName())
                        .path(jobId + "-metadata")
                        .build().toString();
            resultUrls.add(metadataUrl);
        }

        return resultUrls;
    }

    void generateProductMetadata(String jobId, String userName, String hostName, int portNumber)
                throws ProductMetadataException {
        LocalJob job = GpfProductionService.getProductionStatusMap().get(jobId);
        if (job == null) {
            throw new ProductMetadataException("Unable to create metadata for jobId '" + jobId + "'");
        }
        try {
            String processId = (String) job.getParameters().get("processId");
            ProcessorNameConverter processorNameConverter = new ProcessorNameConverter(processId);
            LocalProcessorExtractor processorExtractor = new LocalProcessorExtractor();
            WpsProcess processor = processorExtractor.getProcessor(processorNameConverter, userName);
            String targetDirPath = (String) job.getParameters().get("targetDir");
            File targetDir = new File(targetDirPath);
            doGenerateProductMetadata(targetDir, job.getId(), job.getParameters(), processor, hostName, portNumber);
        } catch (InvalidProcessorIdException | ProductionException | FileNotFoundException | WpsProcessorNotFoundException exception) {
            throw new ProductMetadataException(exception);
        }
    }

    private void doGenerateProductMetadata(File targetDir,
                                           String jobid,
                                           Map<String, Object> processParameters,
                                           WpsProcess processor,
                                           String hostName,
                                           int portNumber)
                throws ProductionException, ProductMetadataException, FileNotFoundException {
        File outputMetadata = new File(targetDir, jobid + "-metadata");
        if (outputMetadata.exists()) {
            return;
        }
        File[] resultProductFiles = targetDir.listFiles();
        String stagingPaths[] = targetDir.getAbsolutePath().split("[/\\\\]staging[/\\\\]");


        ProductMetadata productMetadata = ProductMetadataBuilder.create()
                    .isLocal()
                    .withProductionResults(resultProductFiles != null ? Arrays.asList(resultProductFiles) : new ArrayList<>())
                    .withProcessParameters(processParameters)
                    .withProductOutputDir(stagingPaths[stagingPaths.length - 1])
                    .withProcessor(processor)
                    .withHostName(hostName)
                    .withPortNumber(portNumber)
                    .build();

        VelocityWrapper velocityWrapper = new VelocityWrapper();
        String mergedMetadata = velocityWrapper.merge(productMetadata.getContextMap(), PropertiesWrapper.get("metadata.template"));

        PrintWriter out = new PrintWriter(outputMetadata.getAbsolutePath());
        out.println(mergedMetadata);
        out.close();
    }
}
