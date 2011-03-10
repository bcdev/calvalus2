package com.bc.calvalus.staging;

import java.io.File;
import java.io.IOException;

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
}
