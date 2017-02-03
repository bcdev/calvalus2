package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.exceptions.ProductMetadataException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.localprocess.LocalProductionStatus;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
import com.bc.wps.api.WpsRequestContext;
import com.bc.wps.api.schema.Execute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This is the facade of any calvalus-related operations.
 *
 * @author hans
 */
public class CalvalusFacade extends ProcessFacade {

    private final CalvalusProduction calvalusProduction;
    private final CalvalusStaging calvalusStaging;
    private final CalvalusProcessorExtractor calvalusProcessorExtractor;

    public CalvalusFacade(WpsRequestContext wpsRequestContext) throws IOException {
        super(wpsRequestContext);
        this.calvalusProduction = new CalvalusProduction();
        this.calvalusStaging = new CalvalusStaging(wpsRequestContext.getServerContext());
        this.calvalusProcessorExtractor = new CalvalusProcessorExtractor();
    }

    @Override
    public LocalProductionStatus orderProductionAsynchronous(Execute executeRequest) throws WpsProductionException {
        return calvalusProduction.orderProductionAsynchronous(executeRequest, systemUserName, this);
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        return calvalusProduction.orderProductionSynchronous(executeRequest, systemUserName, this);
    }

    @Override
    public List<String> getProductResultUrls(String jobId) throws WpsResultProductException {
        return calvalusStaging.getProductResultUrls(jobId, CalvalusProductionService.getDefaultConfig());
    }

    @Override
    public void stageProduction(String jobId) throws WpsStagingException {
        calvalusStaging.stageProduction(jobId);
    }

    @Override
    public void observeStagingStatus(String jobId) throws WpsStagingException {
        calvalusStaging.observeStagingStatus(jobId);
    }

    @Override
    public void generateProductMetadata(String jobId) throws ProductMetadataException {
        calvalusStaging.generateProductMetadata(jobId, CalvalusProductionService.getDefaultConfig());
    }

    @Override
    public List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        return calvalusProcessorExtractor.getProcessors(remoteUserName);
    }

    @Override
    public WpsProcess getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        return calvalusProcessorExtractor.getProcessor(parser, remoteUserName);
    }

    public ProductSet[] getProductSets() throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        InventoryService inventoryService = getServices().getInventoryService();
        productSets.addAll(Arrays.asList(inventoryService.getProductSets(remoteUserName, "")));
        productSets.addAll(Arrays.asList(inventoryService.getProductSets(remoteUserName, "user=" + remoteUserName)));
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    public Production getProduction(String jobId) throws IOException, ProductionException {
        return getServices().getProductionService().getProduction(jobId);
    }

    private ServiceContainer getServices() throws ProductionException, IOException {
        return CalvalusProductionService.getServiceContainerSingleton();
    }
}
