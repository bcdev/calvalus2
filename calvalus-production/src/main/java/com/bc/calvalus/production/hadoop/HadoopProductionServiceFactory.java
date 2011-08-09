package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.inventory.hadoop.HadoopInventoryService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.ProductionStore;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.SimpleProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Creates a hadoop production service.
 */
public class HadoopProductionServiceFactory implements ProductionServiceFactory {
    private static final String DEFAULT_PRODUCTIONS_DB_FILENAME = "calvalus-productions-db.csv";

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    File localContextDir,
                                    File localStagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        final File calvalusDataDir = getCalvalusDataDir(localContextDir);

        JobConf jobConf = createJobConf(serviceConfiguration);
        try {
            JobClient jobClient = new JobClient(jobConf);
            HadoopInventoryService inventoryService = new HadoopInventoryService(jobClient.getFs());
            HadoopProcessingService processingService = new HadoopProcessingService(jobClient);
            ProductionStore productionStore = new SimpleProductionStore(processingService.getJobIdFormat(),
                                                                        new File(calvalusDataDir, DEFAULT_PRODUCTIONS_DB_FILENAME));
            StagingService stagingService = new SimpleStagingService(localStagingDir, 3);
            ProductionType l2ProductionType = new L2ProductionType(inventoryService, processingService, stagingService);
            ProductionType l3ProductionType = new L3ProductionType(inventoryService, processingService, stagingService);
            ProductionType taProductionType = new TAProductionType(inventoryService, processingService, stagingService);
            ProductionType maProductionType = new MAProductionType(inventoryService, processingService, stagingService);
            return new ProductionServiceImpl(inventoryService,
                                             processingService,
                                             stagingService,
                                             productionStore,
                                             l2ProductionType,
                                             l3ProductionType,
                                             taProductionType,
                                             maProductionType);
        } catch (IOException e) {
            throw new ProductionException("Failed to create Hadoop JobClient." + e.getMessage(), e);
        }
    }

    private File getCalvalusDataDir(File localContextDir) {
        final File calvalusDataDir;
        final String userHome = System.getProperty("user.home");
        if (userHome != null) {
            calvalusDataDir = new File(userHome, ".calvalus");
            if (!calvalusDataDir.exists()) {
                calvalusDataDir.mkdir();
            }
        } else {
            calvalusDataDir = localContextDir;
        }
        return calvalusDataDir;
    }

    private static JobConf createJobConf(Map<String, String> hadoopProp) {
        JobConf jobConf = new JobConf();
        for (Map.Entry<String, String> entry : hadoopProp.entrySet()) {
            String name = entry.getKey();
            if (name.startsWith("calvalus.hadoop.")) {
                String hadoopName = name.substring("calvalus.hadoop.".length());
                jobConf.set(hadoopName, entry.getValue());
                // System.out.println("Using Hadoop configuration: " + hadoopName + " = " + hadoopValue);
            }
        }
        return jobConf;
    }

}
