package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.shared.BundleFilter;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.MaskDescriptor;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.wps.db.SqlStore;

import java.io.OutputStream;

/**
 * @author hans
 */
public class LocalProductionServiceImpl implements ProductionService {

    private final SqlStore localJobStore;

    public LocalProductionServiceImpl(SqlStore localJobStore) {
        this.localJobStore = localJobStore;
    }

    @Override
    public ProductSet[] getProductSets(String userName, String filter) throws ProductionException {
        return new ProductSet[0];
    }

    @Override
    public BundleDescriptor[] getBundles(String username, BundleFilter filter) throws ProductionException {
        return new BundleDescriptor[0];
    }

    @Override
    public MaskDescriptor[] getMasks(String userName) throws ProductionException {
        return new MaskDescriptor[0];
    }

    @Override
    public LocalJob[] getProductions(String filter) throws ProductionException {
        return localJobStore.getProductions();
    }

    @Override
    public Production getProduction(String id) throws ProductionException {
        return localJobStore.getProduction(id);
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest request) throws ProductionException {
        LocalProductionStatus status = new LocalProductionStatus(processBuilder.getJobId(),
                                                                 ProcessState.SCHEDULED,
                                                                 0,
                                                                 "The request has been queued.",
                                                                 null);
        LocalJob job = new LocalJob(processBuilder.getJobId(), processBuilder.getParameters(), status);
        localJobStore.addProduction(production);
        GpfProductionService.getProductionStatusMap().put(processBuilder.getJobId(), job);
        GpfTask gpfTask = new GpfTask(localFacade, processBuilder);
        GpfProductionService.getWorker().submit(gpfTask);

        return null;
    }

    @Override
    public void cancelProductions(String... productionIds) throws ProductionException {

    }

    @Override
    public void deleteProductions(String... productionIds) throws ProductionException {

    }

    @Override
    public void stageProductions(String... productionIds) throws ProductionException {

    }

    @Override
    public void scpProduction(String productionId, String remotePath) throws ProductionException {

    }

    @Override
    public void updateStatuses(String username) {

    }

    @Override
    public void close() throws ProductionException {

    }

    @Override
    public String[] listUserFiles(String userName, String glob) throws ProductionException {
        return new String[0];
    }

    @Override
    public String[] listSystemFiles(String userName, String glob) throws ProductionException {
        return new String[0];
    }

    @Override
    public OutputStream addUserFile(String userName, String path) throws ProductionException {
        return null;
    }

    @Override
    public boolean removeUserFile(String userName, String path) throws ProductionException {
        return false;
    }

    @Override
    public boolean removeUserDirectory(String userName, String path) throws ProductionException {
        return false;
    }

    @Override
    public String getQualifiedUserPath(String userName, String filePath) throws ProductionException {
        return null;
    }

    @Override
    public String getQualifiedPath(String userName, String filePath) throws ProductionException {
        return null;
    }
}
