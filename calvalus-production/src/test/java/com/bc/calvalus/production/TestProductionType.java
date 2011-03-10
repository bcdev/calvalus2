package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;

import java.io.IOException;

public class TestProductionType implements ProductionType {
    int productionCount;
    private final StagingService stagingService;

    public TestProductionType(StagingService stagingService) {
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        productionCount++;
        return new Production("id_" + productionCount,
                              "name_" + productionCount,
                              "user_" + productionCount,
                              "stagingPath_" + productionCount,
                              productionRequest,
                              "job_" + productionCount + "_1",
                              "job_" + productionCount + "_2");
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        Staging staging = new Staging() {

            @Override
            public String call() throws Exception {
                return null;
            }

        };
        try {
            stagingService.submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(e);
        }
        return staging;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return "test".equals(productionRequest.getProductionType());
    }
}
