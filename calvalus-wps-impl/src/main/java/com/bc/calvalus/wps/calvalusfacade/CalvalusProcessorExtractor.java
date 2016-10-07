package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.ProcessorExtractor;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;

import java.io.IOException;

/**
 * This class handles the processor lookup operation.
 *
 * @author hans
 */
class CalvalusProcessorExtractor extends ProcessorExtractor {

    protected BundleDescriptor[] getBundleDescriptors(String userName) throws WpsProcessorNotFoundException {
        try {
            ProductionService productionService = CalvalusProductionService.getProductionServiceSingleton();
            BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER").withTheUser(userName);
            return productionService.getBundles(userName, filter);
        } catch (IOException | ProductionException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }
}
