package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.db.SqlStore;
import com.bc.calvalus.wps.exceptions.SqlStoreException;

import java.io.File;
import java.util.Map;

/**
 * @author hans
 */
class LocalWpsProductionServiceFactory {

    private static final String DEFAULT_PRODUCTIONS_DB_NAME = "local-database";

    public LocalProductionService create(Map<String, String> serviceConfiguration, File appDataDir, File localStagingDir)
                throws SqlStoreException {
        final SqlStore LocalJobStore;
        String databaseUrl = "jdbc:hsqldb:file:" + new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME).getPath();
        boolean databaseExists = new File(appDataDir, DEFAULT_PRODUCTIONS_DB_NAME + ".properties").exists();
        LocalJobStore = SqlStore.create("org.hsqldb.jdbcDriver",
                                        databaseUrl,
                                        "SA", "",
                                        !databaseExists);
        return new LocalProductionService(LocalJobStore);
    }
}
