package com.bc.calvalus.production;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Responsible for creating ProductionService instances.
 */
public interface ProductionServiceFactory {
    ProductionService create(Map<String, String> serviceConfiguration, Logger logger, File outputDir) throws ProductionException;
}
