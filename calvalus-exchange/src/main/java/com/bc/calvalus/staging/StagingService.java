package com.bc.calvalus.staging;

import java.io.IOException;

/**
 * The interface to the Calvalus staging service.
 *
 * @author Norman
 */
public interface StagingService {
    /**
     * @return The absolute path to the staging area.
     */
    String getStagingAreaPath();

    void orderStaging(Staging staging) throws IOException;
}
