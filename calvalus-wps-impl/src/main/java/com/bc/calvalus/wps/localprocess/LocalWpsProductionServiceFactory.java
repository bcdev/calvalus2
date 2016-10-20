package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.wps.db.SqlStore;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * @author hans
 */
public class LocalWpsProductionServiceFactory implements ProductionServiceFactory {

    private static final String DEFAULT_PRODUCTIONS_DB_NAME = "local-database";

    @Override
    public ProductionService create(Map<String, String> serviceConfiguration, File appDataDir, File localStagingDir)
                throws ProductionException, IOException {
        final SqlStore LocalJobStore;
        String databaseUrl = "jdbc:hsqldb:file:" + new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME).getPath();
        boolean databaseExists = new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME + ".properties").exists();
        LocalJobStore = SqlStore.create("org.hsqldb.jdbcDriver",
                                        databaseUrl,
                                        "SA", "",
                                        !databaseExists);
        return new LocalProductionServiceImpl(LocalJobStore);
    }
}
