package com.bc.calvalus.production.hadoop;

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
    private static final File DEFAULT_PRODUCTIONS_DB_FILE = new File("calvalus-productions-db.csv");
    private static final int PRODUCTION_STATUS_OBSERVATION_PERIOD = 2000;

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    String localStagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        JobConf jobConf = createJobConf(serviceConfiguration);
        try {
            JobClient jobClient = new JobClient(jobConf);
            HadoopProcessingService processingService = new HadoopProcessingService(jobClient);
            ProductionStore productionStore = new SimpleProductionStore(processingService.getJobIdFormat(),
                                                                        DEFAULT_PRODUCTIONS_DB_FILE);
            StagingService stagingService = new SimpleStagingService(localStagingDir, 3);
            ProductionType l2ProductionType = new L2ProductionType(processingService, stagingService);
            ProductionType l3ProductionType = new L3ProductionType(processingService, stagingService);
            ProductionServiceImpl productionService = new ProductionServiceImpl(processingService, stagingService, productionStore,
                                                                                l2ProductionType,
                                                                                l3ProductionType);

            productionService.startStatusObserver(PRODUCTION_STATUS_OBSERVATION_PERIOD);
            return productionService;
        } catch (IOException e) {
            throw new ProductionException("Failed to create Hadoop JobClient." + e.getMessage(), e);
        }
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
