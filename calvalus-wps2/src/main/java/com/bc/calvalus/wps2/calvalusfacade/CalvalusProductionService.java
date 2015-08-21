package com.bc.calvalus.wps2.calvalusfacade;

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

    private static final String CATALINA_BASE = System.getProperty("catalina.base");
    private static final String WEBAPP_ROOT = CATALINA_BASE + "/webapps/ROOT/";
    private static final String DEFAULT_CONFIG_PATH = new File(ProductionServiceConfig.getUserAppDataDir(),
                                                               "calvalus.config").getPath();

    public ProductionService createProductionService(CalvalusConfig calvalusConfig) throws ProductionException, IOException {
        HadoopProductionServiceFactory productionServiceFactory = new HadoopProductionServiceFactory();
        Map<String, String> defaultConfig = calvalusConfig.getDefaultConfig();
        Map<String, String> config = ProductionServiceConfig.loadConfig(new File(DEFAULT_CONFIG_PATH), defaultConfig);
        return productionServiceFactory
                    .create(config, ProductionServiceConfig.getUserAppDataDir(), new File(WEBAPP_ROOT, config.get("calvalus.wps.staging.path")));
    }

}
