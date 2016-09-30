package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.wps.calvalusfacade.WpsProcess;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.utils.ProductMetadata;
import com.bc.calvalus.wps.utils.ProductMetadataBuilder;
import com.bc.calvalus.wps.utils.VelocityWrapper;
import com.bc.wps.utilities.PropertiesWrapper;
import com.bc.wps.utilities.WpsLogger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author hans
 */
public class LocalMetadata {

    private Logger logger = WpsLogger.getLogger();

    public void generateProductMetadata(File targetDir,
                                         String jobid,
                                         Map<String, Object> processParameters,
                                         WpsProcess processor,
                                         String hostName,
                                         int portNumber)
                throws ProductionException, ProductMetadataException {
        File outputMetadata = new File(targetDir, jobid + "-metadata");
        if (outputMetadata.exists()) {
            return;
        }
        File[] resultProductFiles = targetDir.listFiles();
        String stagingDirectoryName = targetDir.getParentFile().getName() + "/" + targetDir.getName();

        ProductMetadata productMetadata = ProductMetadataBuilder.create()
                    .isLocal()
                    .withProductionResults(resultProductFiles != null ? Arrays.asList(resultProductFiles) : new ArrayList<>())
                    .withProcessParameters(processParameters)
                    .withProductOutputDir(stagingDirectoryName)
                    .withProcessor(processor)
                    .withHostName(hostName)
                    .withPortNumber(portNumber)
                    .build();

        VelocityWrapper velocityWrapper = new VelocityWrapper();
        String mergedMetadata = velocityWrapper.merge(productMetadata.getContextMap(), PropertiesWrapper.get("metadata.template"));

        try (PrintWriter out = new PrintWriter(outputMetadata.getAbsolutePath())) {
            out.println(mergedMetadata);
        } catch (FileNotFoundException exception) {
            logger.log(Level.SEVERE, "Unable to write metadata file '" + outputMetadata + "'.", exception);
        }
    }

}
