package com.bc.calvalus.wps.localprocess;

import com.bc.calvalus.wps.db.SqlStore;
import com.bc.calvalus.wps.exceptions.SqlStoreException;

/**
 * @author hans
 */
public class LocalProductionService {

    private final SqlStore localJobStore;

    LocalProductionService(SqlStore localJobStore) {
        this.localJobStore = localJobStore;
    }

    public synchronized LocalJob[] getJobs() {
        return localJobStore.getJobs();
    }

    public LocalJob getJob(String id) {
        return localJobStore.getJob(id);
    }

    void addJob(LocalJob job) {
        localJobStore.addJob(job);
    }

    void updateJob(LocalJob job) {
        localJobStore.updateJob(job);
    }

    void deleteJob(LocalJob job) {
        localJobStore.removeJob(job.getId());
    }

    void updateStatuses() throws SqlStoreException {
        localJobStore.persist();
    }

    void close() throws SqlStoreException {
        localJobStore.close();
    }
}
