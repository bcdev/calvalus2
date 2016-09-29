package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.ProcessFacade;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;
import com.bc.calvalus.wps.exceptions.WpsProductionException;
import com.bc.calvalus.wps.exceptions.WpsResultProductException;
import com.bc.calvalus.wps.exceptions.WpsStagingException;
import com.bc.calvalus.wps.utils.ProcessorNameConverter;
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

    public String orderProductionAsynchronous(ProductionRequest request) throws WpsProductionException {
        try {
            return calvalusProduction.orderProductionAsynchronous(getProductionService(), request, userName);
        } catch (ProductionException | IOException exception) {
            throw new WpsProductionException(exception);
        }
    }

    public String orderProductionSynchronous(ProductionRequest request) throws WpsProductionException {
        try {
            return calvalusProduction.orderProductionSynchronous(getProductionService(), request);
        } catch (ProductionException | IOException | InterruptedException exception) {
            throw new WpsProductionException(exception);
        }
    }

    public List<String> getProductResultUrls(String jobId) throws WpsResultProductException {
        try {
            Production production = getProduction(jobId);
            return calvalusStaging.getProductResultUrls(CalvalusProductionService.getDefaultConfig(), production);
        } catch (ProductionException | IOException exception) {
            throw new WpsResultProductException(exception);
        }
    }

    public void stageProduction(String jobId) throws WpsStagingException {
        try {
            calvalusStaging.stageProduction(getProductionService(), jobId);
        } catch (ProductionException | IOException exception) {
            throw new WpsStagingException(exception);
        }
    }

    public void observeStagingStatus(String jobId) throws WpsStagingException {
        try {
            Production production = getProduction(jobId);
            calvalusStaging.observeStagingStatus(getProductionService(), production);
        } catch (ProductionException | IOException | InterruptedException exception) {
            throw new WpsStagingException(exception);
        }
    }

    public List<IWpsProcess> getProcessors() throws WpsProcessorNotFoundException {
        try {
            return calvalusProcessorExtractor.getProcessors(getProductionService(), userName);
        } catch (ProductionException | IOException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }

    public CalvalusProcessor getProcessor(ProcessorNameConverter parser) throws WpsProcessorNotFoundException {
        try {
            return calvalusProcessorExtractor.getProcessor(parser, getProductionService(), userName);
        } catch (ProductionException | IOException exception) {
            throw new WpsProcessorNotFoundException("Unable to retrieve processor '" + parser.getProcessorIdentifier() + "' from Calvalus.", exception);
        }
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
