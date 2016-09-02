package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.ProductMetadataBuilder;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.api.WpsServerContext;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;
import com.sun.jersey.api.uri.UriBuilderImpl;

import javax.ws.rs.core.UriBuilder;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LocalStaging {

    private static Logger logger = WpsLogger.getLogger();

    public List<String> getProductUrls(String hostAddress, int portNumber, File targetDir, String jobId) {
        List<String> resultUrls = new ArrayList<>();
        String[] resultProductNames = targetDir.list();
        if (resultProductNames != null) {
            for (String filename : resultProductNames) {
                UriBuilder builder = new UriBuilderImpl();
                String productUrl = builder.scheme("http")
                            .host(hostAddress)
                            .port(portNumber)
                            .path(PropertiesWrapper.get("wps.application.name"))
                            .path(PropertiesWrapper.get("utep.output.directory"))
                            .path(targetDir.getParentFile().getName())
                            .path(targetDir.getName())
                            .path(filename).build().toString();
                resultUrls.add(productUrl);
            }

            generateProductMetadata(stagingDirectory, resultUrls);

            UriBuilder builder = new UriBuilderImpl();
            String metadataUrl = builder.scheme("http")
                        .host(hostAddress)
                        .port(portNumber)
                        .path(PropertiesWrapper.get("wps.application.name"))
                        .path(PropertiesWrapper.get("utep.output.directory"))
                        .path(targetDir.getParentFile().getName())
                        .path(targetDir.getName())
                        .path(jobId + "-metadata")
                        .build().toString();
            resultUrls.add(metadataUrl);
        }

        return resultUrls;
    }

    public void generateProductMetadata(File stagingDirectory, File[] productResultFiles, String jobid) throws ProductionException {
        File outputMetadata = new File(stagingDirectory, jobid + "-metadata");
        if (outputMetadata.exists()) {
            return;
        }

        ProductMetadataBuilder builder = ProductMetadataBuilder.create()
                    .
        ProductMetadata productMetadata = new ProductMetadata(production, Arrays.asList(productResultFiles), wpsServerContext);

        VelocityWrapper velocityWrapper = new VelocityWrapper();
        String mergedMetadata = velocityWrapper.merge(productMetadata.getContextMap(), "metadata-template.vm");

        try (PrintWriter out = new PrintWriter(outputMetadata.getAbsolutePath())) {
            out.println(mergedMetadata);
        } catch (FileNotFoundException exception) {
            logger.log(Level.SEVERE, "Unable to write metadata file '" + outputMetadata + "'.", exception);
        }
    }
}
