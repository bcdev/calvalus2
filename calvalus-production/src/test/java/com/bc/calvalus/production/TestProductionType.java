package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;

public class TestProductionType implements ProductionType {
    int productionCount;

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
        return new Staging() {

            @Override
            public String call() throws Exception {
                return null;
            }

        };
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return "test".equals(productionRequest.getProductionType());
    }
}
