package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by hans on 13/08/2015.
 */
public class CalvalusHelper {

    private final String userName;
    private final CalvalusProduction calvalusProduction;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor calvalusProcessorExtractor;

    public CalvalusHelper(String userName) throws IOException, ProductionException {
        this.userName = userName;
        this.calvalusProduction = new CalvalusProduction();
        this.calvalusStaging = new CalvalusStaging();
        this.calvalusProcessorExtractor = new CalvalusProcessorExtractor(getProductionService(), userName);
    }

    public ProductionService getProductionService() throws ProductionException, IOException {
        return CalvalusProductionService.getProductionServiceSingleton();
    }

    public Production orderProductionAsynchronous(ProductionRequest request) throws ProductionException, InterruptedException, IOException {
        return calvalusProduction.orderProductionAsynchronous(getProductionService(), request, userName);
    }

    public Production orderProductionSynchronous(ProductionRequest request) throws ProductionException, InterruptedException, IOException {
        return calvalusProduction.orderProductionSynchronous(getProductionService(), request);
    }

    public List<String> getProductResultUrls(Production production) throws UnknownHostException {
        return calvalusStaging.getProductResultUrls(CalvalusProductionService.getDefaultConfig(), production);
    }

    public void stageProduction(ProductionService productionService, Production production) throws ProductionException, InterruptedException {
        calvalusStaging.stageProduction(productionService, production);
    }

    public void observeStagingStatus(ProductionService productionService, Production production) throws InterruptedException {
        calvalusStaging.observeStagingStatus(productionService, production);
    }

    public List<Processor> getProcessors() throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessors();
    }

    public Processor getProcessor(ProcessorNameParser parser) throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessor(parser);
    }

    public ProductSet[] getProductSets() throws ProductionException {
        return calvalusProcessorExtractor.getProductSets();
    }

}
