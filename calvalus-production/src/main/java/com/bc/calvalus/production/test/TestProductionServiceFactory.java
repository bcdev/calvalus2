package com.bc.calvalus.production.test;

import com.bc.calvalus.processing.JobIdFormat;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.production.SimpleProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import com.bc.calvalus.staging.Staging;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Factory for the {@link TestProductionService}.
 */
public class TestProductionServiceFactory implements ProductionServiceFactory {
    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    String relStagingUrl,
                                    String localStagingDir) throws ProductionException {

        return new TestProductionService(relStagingUrl, localStagingDir);
    }

}
