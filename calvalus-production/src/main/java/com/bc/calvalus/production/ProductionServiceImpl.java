package com.bc.calvalus.production;


import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.processing.beam.BeamUtils;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.bc.io.IOUtils;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A ProductionService implementation that delegates to a Hadoop cluster.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class ProductionServiceImpl implements ProductionService {

    public static enum Action {
        CANCEL,
        DELETE,
        RESTART,// todo - implement restart
    }

    private final ProcessingService processingService;
    private final StagingService stagingService;
    private final ProductionType[] productionTypes;
    private final ProductionStore productionStore;
    private final Map<String, Action> productionActionMap;
    private final Map<String, Staging> productionStagingsMap;
    private final Logger logger;

    public ProductionServiceImpl(ProcessingService processingService,
                                 StagingService stagingService,
                                 ProductionStore productionStore,
                                 ProductionType... productionTypes) throws ProductionException {
        this.productionStore = productionStore;
        this.processingService = processingService;
        this.stagingService = stagingService;
        this.productionTypes = productionTypes;
        this.productionActionMap = new HashMap<String, Action>();
        this.productionStagingsMap = new HashMap<String, Staging>();
        this.logger = Logger.getLogger("com.bc.calvalus");

        try {
            productionStore.load();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to load productions: " + e.getMessage(), e);
        }
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws ProductionException {
        ArrayList<ProductSet> productSets = new ArrayList<ProductSet>();

        try {
            String inputPath = processingService.getDataInputPath();
            String[] paths = processingService.listFilePaths(inputPath);
            // TODO make this more generic
            String[] reversedPathes = new String[paths.length];
            for (int i = 0; i < reversedPathes.length; i++) {
                reversedPathes[i] = paths[paths.length -i - 1];
            }
            for (String path : reversedPathes) {
                String relPath = path.substring(inputPath.length() + 1);
                String type = relPath.indexOf('/') > 0 ? relPath.substring(0, relPath.indexOf('/')) : relPath;
                String name = relPath;

                String[] subPaths = processingService.listFilePaths(path);

                if (subPaths.length > 1) {
                    productSets.add(new ProductSet(relPath, type, name));
                }
                for (String subPath : subPaths) {
                    String subRelPath = subPath.substring(inputPath.length() + 1);
                    String subName = subRelPath;
                    productSets.add(new ProductSet(subRelPath, type, subName));
                }

            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }
        postProcessProductSets(productSets);
        return productSets.toArray(new ProductSet[productSets.size()]);
    }

    // TODO make this more generic
    private ArrayList<ProductSet> postProcessProductSets(ArrayList<ProductSet> productSets) {
        Calendar calendar = ProductData.UTC.createCalendar();
        ArrayList<ProductSet> result = new ArrayList<ProductSet>();
        for (ProductSet productSet : productSets) {
            if (productSet.getName().contains("_RR_")) {
                calendar.set(2004, 00, 01);
                productSet.setMinDate(calendar.getTime());
                calendar.set(2008, 11, 31);
                productSet.setMaxDate(calendar.getTime());
            } else if (productSet.getName().contains("_FSG_")) {
                calendar.set(2005, 00, 01);
                productSet.setMinDate(calendar.getTime());
                calendar.set(2009, 11, 31);
                productSet.setMaxDate(calendar.getTime());
            }
        }

        return result;
    }

    @Override
    public ProcessorDescriptor[] getProcessors(String filter) throws ProductionException {
        ArrayList<ProcessorDescriptor> descriptors = new ArrayList<ProcessorDescriptor>();

        try {
            String softwarePath = processingService.getSoftwarePath();
            String[] paths = processingService.listFilePaths(softwarePath);
            for (String path : paths) {
                String[] subPaths = processingService.listFilePaths(path);
                for (String subPath : subPaths) {
                    if (subPath.endsWith("processor-descriptor.xml")) {
                        try {
                            InputStream is = processingService.open(subPath);
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            IOUtils.copyBytes(is, baos);
                            String xmlContent = baos.toString();
                            ProcessorDescriptor pd = new ProcessorDescriptor();
                            BeamUtils.convertXmlToObject(xmlContent, pd);
                            descriptors.add(pd);
                        } catch (Exception e) {
                            logger.warning(e.getMessage());
                        }
                    }
                }
            }
        } catch (IOException e) {
            logger.warning(e.getMessage());
        }

        return descriptors.toArray(new ProcessorDescriptor[descriptors.size()]);
    }

    @Override
    public synchronized Production[] getProductions(String filter) throws ProductionException {
        return productionStore.getProductions();
    }

    @Override
    public Production getProduction(String id) throws ProductionException {
        return productionStore.getProduction(id);
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {
        logger.info("orderProduction:");
        logger.info("user: " + productionRequest.getUserName());
        logger.info("type: " + productionRequest.getProductionType());
        for (Map.Entry<String, String> entry : productionRequest.getParameters().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            logger.info(key + "=" + value);
        }
        logger.info("type: " + productionRequest.getParameters());

        ProductionType productionType = findProductionType(productionRequest);
        synchronized (this) {
            Production production = productionType.createProduction(productionRequest);
            try {
                production.getWorkflow().submit();
            } catch (WorkflowException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
                throw new ProductionException(String.format("Failed to submit production '%s': %s", production.getId(), e.getMessage()), e);
            }
            productionStore.addProduction(production);
            return new ProductionResponse(production);
        }
    }

    @Override
    public synchronized void stageProductions(String... productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            Production production = productionStore.getProduction(productionId);
            if (production != null) {
                try {
                    stageProductionResults(production);
                    count++;
                } catch (ProductionException e) {
                    logger.log(Level.SEVERE, String.format("Failed to stage production '%s': %s",
                                                           production.getId(), e.getMessage()), e);
                }
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been staged.", count, productionIds.length));
        }
    }

    @Override
    public void cancelProductions(String... productionIds) throws ProductionException {
        requestProductionKill(productionIds, Action.CANCEL);
    }

    @Override
    public void deleteProductions(String... productionIds) throws ProductionException {
        requestProductionKill(productionIds, Action.DELETE);
    }

    private void requestProductionKill(String[] productionIds, Action action) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            Production production = productionStore.getProduction(productionId);
            if (production != null) {
                productionActionMap.put(production.getId(), action);

                Staging staging = productionStagingsMap.get(production.getId());
                if (staging != null && !staging.isCancelled()) {
                    productionStagingsMap.remove(production.getId());
                    staging.cancel();
                }

                if (production.getProcessingStatus().isDone()) {
                    if (action == Action.DELETE) {
                        removeProduction(production);
                    }
                } else {
                    try {
                        production.getWorkflow().kill();
                    } catch (WorkflowException e) {
                        logger.log(Level.SEVERE, String.format("Failed to kill production '%s': %s",
                                                               production.getId(), e.getMessage()), e);
                    }
                }

                count++;
            } else {
                logger.warning(String.format("Failed to kill unknown production '%s'", productionId));
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been killed. See server log for details.",
                                                        count, productionIds.length));
        }
    }

    private void stageProductionResults(Production production) throws ProductionException {
        production.setStagingStatus(ProcessStatus.SCHEDULED);
        ProductionType productionType = findProductionType(production.getProductionRequest());
        Staging staging = productionType.createStaging(production);
        productionStagingsMap.put(production.getId(), staging);
    }

    @Override
    public void updateStatuses() {
        try {
            processingService.updateStatuses();
        } catch (IOException e) {
            logger.warning("Failed to update job statuses: " + e.getMessage());
        }

        // Update state of all registered productions
        Production[] productions = productionStore.getProductions();
        for (Production production : productions) {
            production.getWorkflow().updateStatus();
        }

        // Now try to delete productions
        for (Production production : productions) {
            if (production.getProcessingStatus().isDone()) {
                Action action = productionActionMap.get(production.getId());
                if (action == Action.DELETE) {
                    removeProduction(production);
                }
            }
        }

        // Copy result to staging area
        for (Production production : productions) {
            if (production.isAutoStaging()
                    && production.getProcessingStatus().getState() == ProcessState.COMPLETED
                    && production.getStagingStatus().getState() == ProcessState.UNKNOWN
                    && productionStagingsMap.get(production.getId()) == null) {
                try {
                    stageProductionResults(production);
                } catch (ProductionException e) {
                    logger.warning("Failed to stage production: " + e.getMessage());
                }
            }
        }

        // write to persistent storage
        try {
            productionStore.store();
        } catch (IOException e) {
            logger.warning("Failed to persist productions: " + e.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        try {
            stagingService.close();
        } finally {
            try {
                processingService.close();
            } finally {
                productionStore.close();
            }
        }
    }

    private ProductionType findProductionType(ProductionRequest productionRequest) throws ProductionException {
        for (ProductionType productionType : productionTypes) {
            if (productionType.getName().equals(productionRequest.getProductionType())) {
                return productionType;
            }
        }
        for (ProductionType productionType : productionTypes) {
            if (productionType.accepts(productionRequest)) {
                return productionType;
            }
        }
        throw new ProductionException(String.format("Unhandled production request of type '%s'",
                                                    productionRequest.getProductionType()));
    }

    private synchronized void removeProduction(Production production) {
        productionStore.removeProduction(production);
        productionActionMap.remove(production.getId());
        productionStagingsMap.remove(production.getId());
        try {
            stagingService.deleteTree(production.getStagingPath());
        } catch (IOException e) {
            logger.log(Level.SEVERE, String.format("Failed to delete staging directory '%s' of production '%s': %s",
                                                   production.getStagingPath(), production.getId(), e.getMessage()), e);
        }
    }


}
