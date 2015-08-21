package com.bc.calvalus.wps2.calvalusfacade;

import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.ProductionServiceConfig;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * Created by hans on 10/08/2015.
 */
public class CalvalusConfig {

    private static final String DEFAULT_BEAM_BUNDLE = HadoopProcessingService.DEFAULT_BEAM_BUNDLE;
    private static final String DEFAULT_CALVALUS_BUNDLE = HadoopProcessingService.DEFAULT_CALVALUS_BUNDLE;
    private static final String STAGING_DIRECTORY = "staging";

    public Map<String, String> getDefaultConfig() {
        Map<String, String> defaultConfig = ProductionServiceConfig.getCalvalusDefaultConfig();
        defaultConfig.put("production.db.type", "memory");
        defaultConfig.put("calvalus.calvalus.bundle", DEFAULT_CALVALUS_BUNDLE);
        defaultConfig.put("calvalus.beam.bundle", DEFAULT_BEAM_BUNDLE);
        defaultConfig.put("calvalus.wps.staging.path", STAGING_DIRECTORY);
        return defaultConfig;
    }

    private String getConfig(String customConfig, String defaultConfig) {
        return (!StringUtils.isBlank(customConfig)) ? customConfig : defaultConfig;
    }
}
