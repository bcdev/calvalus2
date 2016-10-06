package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
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
        return calvalusProduction.orderProductionAsynchronous(executeRequest, userName, this);
    }

    @Override
    public LocalProductionStatus orderProductionSynchronous(Execute executeRequest) throws WpsProductionException {
        return calvalusProduction.orderProductionSynchronous(executeRequest, userName, this);
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

    }

    @Override
    public List<WpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        return calvalusProcessorExtractor.getProcessors(userName);
    }

    @Override
    public CalvalusProcessor getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        return calvalusProcessorExtractor.getProcessor(parser, userName);
    }

    public ProductSet[] getProductSets() throws ProductionException, IOException {
        List<ProductSet> productSets = new ArrayList<>();
        productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "")));
        try {
            productSets.addAll(Arrays.asList(getProductionService().getProductSets(userName, "user=" + userName)));
        } catch (ProductionException ignored) {
        }
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    public Production getProduction(String jobId) throws IOException, ProductionException {
        return getProductionService().getProduction(jobId);
    }

    private ProductionService getProductionService() throws ProductionException, IOException {
        return CalvalusProductionService.getProductionServiceSingleton();
    }
}
