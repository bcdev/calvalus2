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

    // todo - would be better to use ProductionServiceImpl so that we all its logic in place (nf)
    public ProductionService create2(Map<String, String> serviceConfiguration,
                                    String relStagingUrl,
                                    String localStagingDir) throws ProductionException {
        MyProcessingService processingService = new MyProcessingService(localStagingDir);
        ProductionServiceImpl productionService = new ProductionServiceImpl(processingService, new SimpleStagingService(1), new SimpleProductionStore(processingService.getJobIdFormat(),
                                                                                                      new File("test-productions.csv")),
                                                                            new MyProductionType());
        productionService.startStatusObserver(2000);
        return productionService;

    }

    private static class MyProcessingService implements ProcessingService {
        private final String localStagingDir;

        public MyProcessingService(String localStagingDir) {

            this.localStagingDir = localStagingDir;
        }

        @Override
        public JobIdFormat getJobIdFormat() {
            return JobIdFormat.STRING;
        }

        @Override
        public String getDataArchiveRootPath() {
            return new File(System.getProperty("user.home"), ".calvalus/eodata").getPath();
        }

        @Override
        public String getDataOutputRootPath() {
            return localStagingDir;
        }

        @Override
        public String[] listFilePaths(String dirPath) throws IOException {
            return new File(dirPath).list();
        }

        @Override
        public Map getJobStatusMap() throws IOException {
            return null;
        }

        @Override
        public boolean killJob(Object jobId) throws IOException {
            return false;
        }
    }

    private static class MyProductionType implements ProductionType {
        @Override
        public String getName() {
            return null;
        }

        @Override
        public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
            return null;
        }

        @Override
        public Staging createStaging(Production production) throws ProductionException {
            return null;
        }
    }
}
