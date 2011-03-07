package com.bc.calvalus.production.test;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;

import java.io.File;
import java.util.Map;

/**
 * Factory for the {@link TestProductionService}.
 */
public class TestProductionServiceFactory implements ProductionServiceFactory {
    @Override
    public ProductionService create(Map<String, String> serviceConfiguration, String relStagingUrl, File localStagingDir) throws ProductionException {
        return new TestProductionService(relStagingUrl, localStagingDir);
    }
}
