package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.processing.beam.BeamL3Config;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;

import java.util.HashMap;
import java.util.Map;

// todo - implement this class in order to generate WPS XML
abstract class L2ProcessingRequest {
    private final ProductionRequest productionRequest;

    protected L2ProcessingRequest(ProductionRequest productionRequest) {
        this.productionRequest = productionRequest;
    }

    public Map<String, Object> getProcessingParameters() throws ProductionException {

        throw new IllegalStateException("not implemented");
    }

}
