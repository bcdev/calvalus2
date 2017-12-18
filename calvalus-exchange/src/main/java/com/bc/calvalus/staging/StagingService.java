package com.bc.calvalus.staging;

import java.io.File;
import java.io.IOException;
import java.util.Observable;

/**
 * The interface to the Calvalus staging service.
 *
 * @author Norman
 */
public interface StagingService {
    /**
     * @return The absolute, local directory path to this service's staging area.
     */
    File getStagingDir();


    void submitStaging(Staging staging) throws IOException;

    /**
     * Recursively deletes a path in the staging area.
     *
     * @param path The path to delete.
     * @throws IOException If an error occurs
     */
    void deleteTree(String path) throws IOException;

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     */
    void close();

    /**
     * Memorises service for notification of observers for final production state changes
     * @param productionService
     */
    void setProductionService(Observable productionService);

    /**
     * Returns the service for the notification of observers.
     * @return the production service
     */
    Observable getProductionService();
}
