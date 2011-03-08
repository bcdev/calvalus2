package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;

public class TestProductionType implements ProductionType {
    int productionCount;
    boolean outputStaging;

    public void setOutputStaging(boolean outputStaging) {
        this.outputStaging = outputStaging;
    }

    public int getProductionCount() {
        return productionCount;
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
                              outputStaging,
                              new Object[] {
                                      "job_" + productionCount + "_1",
                                      "job_" + productionCount + "_2",
                              },
                              productionRequest);
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        return new Staging() {
            private boolean cancelled = true;

            @Override
            public Void call() throws Exception {
                return null;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        };
    }
}
