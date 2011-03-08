package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.hadoop.HadoopJobIdFormat;
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

import java.io.IOException;
import java.util.Map;

/**
 * Creates a hadoop production service.
 */
public class HadoopProductionServiceFactory implements ProductionServiceFactory {

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    String relStagingUrl, String localStagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        JobConf jobConf = createJobConf(serviceConfiguration);
        try {
            JobClient jobClient = new JobClient(jobConf);
            ProductionStore productionStore = new SimpleProductionStore(new HadoopJobIdFormat());
            HadoopProcessingService processingService = new HadoopProcessingService(jobClient);
            StagingService stagingService = new SimpleStagingService(3);
            ProductionType l2ProductionType = new L2ProductionType(processingService, localStagingDir);
            ProductionType l3ProductionType = new L3ProductionType(processingService, localStagingDir);
            return new ProductionServiceImpl(productionStore,
                                             processingService,
                                             stagingService,
                                             l2ProductionType,
                                             l3ProductionType);
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
