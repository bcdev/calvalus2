package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.utils.ProcessorNameParser;
import com.bc.wps.api.WpsRequestContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is the facade of any calvalus-related operations.
 *
 * @author hans
 */
public class CalvalusFacade {

    private final String userName;
    private final CalvalusProduction calvalusProduction;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor calvalusProcessorExtractor;

    public CalvalusFacade(WpsRequestContext wpsRequestContext) {
        this.userName = wpsRequestContext.getUserName();
        this.calvalusProduction = new CalvalusProduction();
        this.calvalusStaging = new CalvalusStaging(wpsRequestContext.getServerContext());
        this.calvalusProcessorExtractor = new CalvalusProcessorExtractor();
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
        return calvalusProcessorExtractor.getProcessors(getProductionService(), userName);
    }

    public CalvalusProcessor getProcessor(ProcessorNameParser parser) throws IOException, ProductionException {
        return calvalusProcessorExtractor.getProcessor(parser, getProductionService(), userName);
    }

    public ProductSet[] getProductSets() throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "")));
        productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "user=" + userName)));
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

}
