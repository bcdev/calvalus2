package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.inventory.hadoop.HadoopInventoryService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.production.store.SqlProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Creates a hadoop production service.
 */
public class HadoopProductionServiceFactory implements ProductionServiceFactory {
    private static final String DEFAULT_PRODUCTIONS_DB_FILENAME = "calvalus-database";

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    File appDataDir,
                                    File stagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        JobConf jobConf = new JobConf(createJobConfiguration(serviceConfiguration));
        try {
            JobClient jobClient = new JobClient(jobConf);
            HadoopInventoryService inventoryService = new HadoopInventoryService(jobClient.getFs());
            HadoopProcessingService processingService = new HadoopProcessingService(jobClient);
            // todo - get the database connect info from configuration
            File databaseFile = new File(appDataDir, DEFAULT_PRODUCTIONS_DB_FILENAME);
            ProductionStore productionStore = SqlProductionStore.create(processingService,
                                                                        "org.hsqldb.jdbcDriver",
                                                                        "jdbc:hsqldb:file:" + databaseFile,
                                                                        "SA", "",
                                                                        !databaseFile.exists());
            StagingService stagingService = new SimpleStagingService(stagingDir, 3);
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

    private static Configuration createJobConfiguration(Map<String, String> serviceConfiguration) {
        Configuration jobConfiguration = new Configuration();
        HadoopProductionType.setJobConfig(jobConfiguration, serviceConfiguration);
        return jobConfiguration;
    }

}
