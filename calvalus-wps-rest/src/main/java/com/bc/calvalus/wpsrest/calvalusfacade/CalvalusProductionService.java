package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceConfig;
import com.bc.calvalus.production.hadoop.HadoopProductionServiceFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusProductionService {

    private static ProductionService productionService = null;

    private static final String DEFAULT_BEAM_BUNDLE = HadoopProcessingService.DEFAULT_BEAM_BUNDLE;
    private static final String DEFAULT_CALVALUS_BUNDLE = HadoopProcessingService.DEFAULT_CALVALUS_BUNDLE;
    private static final String STAGING_DIRECTORY = "staging";
    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String WEBAPP_ROOT = CATALINA_BASE + "/webapps/ROOT/";
    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(),
                                                               "calvalus.config").getPath();

    private CalvalusProductionService() {
    }

    public synchronized static ProductionService getInstance() throws IOException, ProductionException {
        if (productionService == null) {
            productionService = createProductionService();
        }
        return productionService;
    }

    protected static Map<String, String> getDefaultConfig() {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
//        defaultConfig.put("production.db.type", "memory");
        defaultConfig.put("calvalus.calvalus.bundle", DEFAULT_CALVALUS_BUNDLE);
        defaultConfig.put("calvalus.beam.bundle", DEFAULT_BEAM_BUNDLE);
        defaultConfig.put("calvalus.wps.staging.path", STAGING_DIRECTORY);
        return defaultConfig;
    }

    private static ProductionService createProductionService() throws ProductionException, IOException {
        HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
        Map<String, String> defaultConfig = getDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(new File(DEFAULT_CONFIG_PATH), defaultConfig);
        return productionServiceFactory
                    .create(config, ProductionServiceConfig.getUserAppDataDir(), new File(WEBAPP_ROOT, config.get("calvalus.wps.staging.path")));
    }
}
