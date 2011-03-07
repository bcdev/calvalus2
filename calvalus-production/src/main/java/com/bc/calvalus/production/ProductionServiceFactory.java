package com.bc.calvalus.production;

import java.util.Map;

/**
 * Responsible for creating ProductionService instances.
 */
public interface ProductionServiceFactory {
    ProductionService create(Map<String, String> serviceConfiguration,
                             String relStagingUrl,
                             String localStagingDir) throws ProductionException;
}
