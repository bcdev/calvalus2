package com.bc.calvalus.production;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Responsible for creating ServiceContainer instances.
 */
public interface ServiceContainerFactory {
    ServiceContainer create(Map<String, String> serviceConfiguration,
                             File localContextDir,
                             File localStagingDir) throws ProductionException, IOException;
}
