package com.bc.calvalus.production;

import java.io.File;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Responsible for creating ProductionService instances.
 */
public interface ProductionServiceFactory {
    ProductionService create(Map<String, String> serviceConfiguration,
                             String relStagingUrl, File localStagingDir) throws ProductionException;
}
