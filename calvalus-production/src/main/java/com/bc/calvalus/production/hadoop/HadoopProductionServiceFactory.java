package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.hadoop.HdfsInventoryService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.ProductionTypeSpi;
import com.bc.calvalus.production.store.MemoryProductionStore;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.production.store.SqlProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Creates a hadoop production service.
 */
public class HadoopProductionServiceFactory implements ProductionServiceFactory {

    private static final String DEFAULT_PRODUCTIONS_DB_NAME = "calvalus-database";

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    File appDataDir,
                                    File stagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");

        JobConf jobConf = new JobConf(createJobConfiguration(serviceConfiguration));
        try {
            JobClient jobClient = new JobClient(jobConf);
            final InventoryService inventoryService = new HdfsInventoryService(jobClient.getFs());
            final HadoopProcessingService processingService = new HadoopProcessingService(jobClient);
            final ProductionStore productionStore;
            if ("memory".equals(serviceConfiguration.get("production.db.type"))) {
                productionStore = new MemoryProductionStore();
            } else {
                // todo - get the database connect info from configuration
                String databaseUrl = "jdbc:hsqldb:file:" + new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME).getPath();
                boolean databaseExists = new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME + ".properties").exists();
                productionStore = SqlProductionStore.create(processingService,
                                                            "org.hsqldb.jdbcDriver",
                                                            databaseUrl,
                                                            "SA", "",
                                                            !databaseExists);
            }
            StagingService stagingService = new SimpleStagingService(stagingDir, 3);
            ProductionType[] productionTypes = getProductionTypes(inventoryService, processingService, stagingService);
            return new ProductionServiceImpl(inventoryService,
                                             processingService,
                                             stagingService,
                                             productionStore,
                                             productionTypes);
        } catch (IOException e) {
            throw new ProductionException("Failed to create Hadoop JobClient." + e.getMessage(), e);
        }
    }

    private static ProductionType[] getProductionTypes(InventoryService inventoryService,
                                  HadoopProcessingService processingService,
                                  StagingService stagingService) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<ProductionTypeSpi> productionTypes = ServiceLoader.load(ProductionTypeSpi.class, contextClassLoader);
        ArrayList<ProductionType> list = new ArrayList<ProductionType>();
        for (ProductionTypeSpi productionType : productionTypes) {
            list.add(productionType.create(inventoryService, processingService, stagingService));
        }
        return list.toArray(new ProductionType[list.size()]);
    }

    private static Configuration createJobConfiguration(Map<String, String> serviceConfiguration) {
        Configuration jobConfiguration = new Configuration();
        HadoopProductionType.setJobConfig(serviceConfiguration, jobConfiguration);
        return jobConfiguration;
    }

}
