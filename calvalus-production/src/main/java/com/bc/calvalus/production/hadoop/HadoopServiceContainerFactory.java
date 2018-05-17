package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.JobClientsMap;
import com.bc.calvalus.inventory.ColorPaletteService;
import com.bc.calvalus.inventory.DefaultColorPaletteService;
import com.bc.calvalus.inventory.DefaultInventoryService;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.hadoop.HdfsFileSystemService;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.ProductionTypeSpi;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.ServiceContainerFactory;
import com.bc.calvalus.production.store.MemoryProductionStore;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.production.store.SqlProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.StagingService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Observable;
import java.util.ServiceLoader;

/**
 * Creates a hadoop production service.
 */
public class HadoopServiceContainerFactory implements ServiceContainerFactory {

    private static final String DEFAULT_PRODUCTIONS_DB_NAME = "calvalus-database";

    @Override
    public ServiceContainer create(Map<String, String> serviceConfiguration,
                                    File appDataDir,
                                    File stagingDir) throws ProductionException {

        // Prevent Windows from using ';' as path separator
        System.setProperty("path.separator", ":");
        String archiveRootDir = serviceConfiguration.getOrDefault("calvalus.portal.archiveRootDir", "eodata");
        String colorPaletteRootDir = serviceConfiguration.getOrDefault("calvalus.portal.colorPaletteRootDir", "auxiliary");
        String softwareDir = serviceConfiguration.getOrDefault("calvalus.portal.softwareDir",
                                                               HadoopProcessingService.CALVALUS_SOFTWARE_PATH);
        // disable cache, otherwise org.apache.hadoop.fs.FileSystem.Cache grows forever
        serviceConfiguration.put("calvalus.hadoop.fs.hdfs.impl.disable.cache", "true");
        // transfer configuration from calvalus.properties to system properties
        if (serviceConfiguration.containsKey("calvalus.accesscontrol.external")) {
            System.setProperty("calvalus.accesscontrol.external",
                               serviceConfiguration.get("calvalus.accesscontrol.external"));
        }

        Configuration hadoopConfiguration = createHadoopConfiguration(serviceConfiguration);
        JobConf jobConf = new JobConf(hadoopConfiguration);
        try {
            JobClientsMap jobClientsMap = new JobClientsMap(jobConf);
            final HdfsFileSystemService hdfsFileSystemService = new HdfsFileSystemService(jobClientsMap);
            final InventoryService inventoryService = new DefaultInventoryService(hdfsFileSystemService, archiveRootDir);
            final ColorPaletteService colorPaletteService = new DefaultColorPaletteService(hdfsFileSystemService, colorPaletteRootDir);
            final HadoopProcessingService processingService = new HadoopProcessingService(jobClientsMap, softwareDir);
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
            ProductionType[] productionTypes = getProductionTypes(hdfsFileSystemService, processingService, stagingService);
            ProductionService productionService = new ProductionServiceImpl(hdfsFileSystemService,
                                                                            processingService,
                                                                            stagingService,
                                                                            productionStore,
                                                                            productionTypes);
            stagingService.setProductionService((Observable) productionService);
            return new ServiceContainer(productionService, hdfsFileSystemService, inventoryService, colorPaletteService, hadoopConfiguration);
        } catch (IOException e) {
            throw new ProductionException("Failed to create Hadoop JobClient." + e.getMessage(), e);
        }
    }

    private static ProductionType[] getProductionTypes(FileSystemService fileSystemServiceService,
                                  HadoopProcessingService processingService,
                                  StagingService stagingService) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader<ProductionTypeSpi> productionTypes = ServiceLoader.load(ProductionTypeSpi.class, contextClassLoader);
        ArrayList<ProductionType> list = new ArrayList<>();
        for (ProductionTypeSpi productionType : productionTypes) {
            list.add(productionType.create(fileSystemServiceService, processingService, stagingService));
        }
        return list.toArray(new ProductionType[list.size()]);
    }

    private static Configuration createHadoopConfiguration(Map<String, String> serviceConfiguration) {
        Configuration jobConfiguration = new Configuration();
        HadoopProductionType.setJobConfig(serviceConfiguration, jobConfiguration);
        return jobConfiguration;
    }

}
