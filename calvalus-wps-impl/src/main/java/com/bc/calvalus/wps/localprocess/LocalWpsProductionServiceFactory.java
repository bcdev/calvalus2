package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.db.SqlStore;
import com.bc.calvalus.wps.exceptions.SqlStoreException;
import com.bc.wps.utilities.PropertiesWrapper;

import java.io.File;

/**
 * @author hans
 */
class LocalWpsProductionServiceFactory {

    private static final String LOCAL_DB_NAME = PropertiesWrapper.get("local.db.name");
    private static final String JDBC_HSQLDB_URL_PREFIX = PropertiesWrapper.get("jdbc.url.prefix");
    private static final String HSQLDB_JDBC_DRIVER = PropertiesWrapper.get("jdbc.driver");

    public LocalProductionService create(File appDataDir)
                throws SqlStoreException {
        final SqlStore LocalJobStore;
        String databaseUrl = JDBC_HSQLDB_URL_PREFIX + new File(appDataDir, LOCAL_DB_NAME).getPath();
        boolean databaseExists = new File(appDataDir, LOCAL_DB_NAME + ".properties").exists();
        LocalJobStore = SqlStore.create(HSQLDB_JDBC_DRIVER,
                                        databaseUrl,
                                        "SA", "",
                                        !databaseExists);
        return new LocalProductionService(LocalJobStore);
    }
}
