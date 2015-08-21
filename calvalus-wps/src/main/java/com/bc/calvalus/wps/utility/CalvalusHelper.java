package com.bc.calvalus.wps.utility;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;

import java.io.File;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by hans on 05/08/2015.
 */
public class CalvalusHelper {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String WEBAPP_ROOT = CATALINA_BASE + "/webapps/ROOT/";

    public ProductionService createProductionService(Map<String, String> config) throws ProductionException {
        HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
        return productionServiceFactory
                    .create(config, ProductionServiceConfig.getUserAppDataDir(), new File(WEBAPP_ROOT, config.get("calvalus.wps.staging.path")));
    }

    private void logError(String errorMessage) {
        LOG.log(Level.SEVERE, errorMessage);
    }

    private void logInfo(String message) {
        LOG.info(message);
    }

}
