package com.bc.calvalus.wps.calvalusfacade;

import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.ProcessorExtractor;
import com.bc.calvalus.wps.exceptions.ProductSetsNotAvailableException;
import com.bc.calvalus.wps.exceptions.WpsProcessorNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * This class handles the processor lookup operation.
 *
 * @author hans
 */
class CalvalusProcessorExtractor extends ProcessorExtractor {

    protected BundleDescriptor[] getBundleDescriptors(String userName) throws WpsProcessorNotFoundException {
        try {
            ProductionService productionService = CalvalusProductionService.getServiceContainerSingleton().getProductionService();
            BundleFilter filter = BundleFilter.fromString("provider=SYSTEM,USER,ALL_USER").withTheUser(userName);
            return productionService.getBundles(userName, filter);
        } catch (IOException | ProductionException exception) {
            throw new WpsProcessorNotFoundException(exception);
        }
    }

    public String[] getRegionFiles(String remoteUserName) throws IOException {
        try {
            List<String> pathPatterns = Collections.singletonList(AbstractFileSystemService.getUserGlob(remoteUserName, "region_data"));
            return CalvalusProductionService.getServiceContainerSingleton().getFileSystemService().globPaths(remoteUserName, pathPatterns);
        } catch (ProductionException e) {
            throw new IOException("failed to retrieve region files", e);
        }
    }
}
