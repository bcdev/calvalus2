package com.bc.calvalus.rest;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.rest.exceptions.CalvalusProcessorNotFoundException;

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

    public ProductSet[] getProductSets(String userName) throws IOException, ProductionException {
        List<ProductSet> productSets = new ArrayList<>();
        InventoryService inventoryService = getServices().getInventoryService();
        productSets.addAll(Arrays.asList(inventoryService.getProductSets(userName, "")));
        productSets.addAll(Arrays.asList(inventoryService.getProductSets(userName, "user=" + userName)));
        productSets.addAll(Arrays.asList(inventoryService.getProductSets(userName, "user=all")));
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    public List<CalvalusProcessor> getProcessors(String userName) throws CalvalusProcessorNotFoundException {
        ProcessorExtractor calvalusProcessorExtractor = new ProcessorExtractor();
        return calvalusProcessorExtractor.getProcessors(userName);
    }

    public CalvalusProcessor getProcessor(ProcessorNameConverter parser, String userName) throws CalvalusProcessorNotFoundException {
        ProcessorExtractor calvalusProcessorExtractor = new ProcessorExtractor();
        return calvalusProcessorExtractor.getProcessor(parser, userName);
    }

    private ServiceContainer getServices() throws ProductionException, IOException {
        return CalvalusProductionService.getServiceContainerSingleton();
    }
}
