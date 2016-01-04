package com.bc.calvalus.wpsrest.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wpsrest.Processor;
import com.bc.calvalus.wpsrest.ProcessorNameParser;
import com.bc.calvalus.wpsrest.ServletRequestWrapper;
import com.bc.calvalus.wpsrest.responses.IWpsProcess;

import java.io.IOException;
import java.util.List;

/**
 * This is the facade of any calvalus-related operations.
 * <p/>
 * Created by hans on 13/08/2015.
 */
public class CalvalusHelper {

    private final String userName;
    private final CalvalusProduction calvalusProduction;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor calvalusProcessorExtractor;

    public CalvalusHelper(ServletRequestWrapper servletRequestWrapper) throws IOException, ProductionException {
        this.userName = servletRequestWrapper.getUserName();
        this.calvalusProduction = new CalvalusProduction();
        this.calvalusStaging = new CalvalusStaging(servletRequestWrapper);
        this.calvalusProcessorExtractor = new CalvalusProcessorExtractor(getProductionService(), userName);
    }

    public ProductionService getProductionService() throws ProductionException, IOException {
        return CalvalusProductionService.getProductionServiceSingleton();
    }

    public Production orderProductionAsynchronous(ProductionRequest request) throws ProductionException, IOException {
        return calvalusProduction.orderProductionAsynchronous(getProductionService(), request, userName);
    }

    public Production orderProductionSynchronous(ProductionRequest request) throws ProductionException, InterruptedException, IOException {
        return calvalusProduction.orderProductionSynchronous(getProductionService(), request);
    }

    public List<String> getProductResultUrls(Production production) {
        return calvalusStaging.getProductResultUrls(CalvalusProductionService.getDefaultConfig(), production);
    }

    public void stageProduction(Production production) throws ProductionException, IOException {
        calvalusStaging.stageProduction(getProductionService(), production);
    }

    public void observeStagingStatus(Production production) throws InterruptedException, IOException, ProductionException {
        calvalusStaging.observeStagingStatus(getProductionService(), production);
    }

    public List<IWpsProcess> getProcessors() throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessors();
    }

    public Processor getProcessor(ProcessorNameParser parser) throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessor(parser);
    }

    public ProductSet[] getProductSets() throws ProductionException, IOException {
        return getProductionService().getProductSets(userName, "");
    }

}
