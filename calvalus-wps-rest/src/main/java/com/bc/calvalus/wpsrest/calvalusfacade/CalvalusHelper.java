package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;

import java.io.IOException;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusHelper {

    private final CalvalusConfig calvalusConfig;
    private final CalvalusProduction calvalusProduction;
    private final CalvalusProductionService calvalusProductionService;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor processorExtractor;

    public CalvalusHelper(CalvalusConfig calvalusConfig, CalvalusProduction calvalusProduction, CalvalusProductionService calvalusProductionService, CalvalusStaging calvalusStaging, CalvalusProcessorExtractor processorExtractor) {
        this.calvalusConfig = calvalusConfig;
        this.calvalusProduction = calvalusProduction;
        this.calvalusProductionService = calvalusProductionService;
        this.calvalusStaging = calvalusStaging;
        this.processorExtractor = processorExtractor;
    }

    public ProductionService createProductionService() throws ProductionException, IOException {
        return CalvalusProductionService.getInstance(calvalusConfig);
    }


}
