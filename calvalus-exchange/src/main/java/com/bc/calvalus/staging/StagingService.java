package com.bc.calvalus.staging;

/**
 * The interface to the Calvalus staging service.
 * <p/>
 * TODO - make use of this interface
 *
 * @author Norman
 */
public interface StagingService {
    /**
     * Gets the product set for the given ID.
     *
     * @param request The staging request.
     * @return The product set, or {@code null} if a product set with the given ID does not exist..
     */
    //StagingResponse orderStaging(StagingRequest request) throws Exception;

    String getAbsolutePath(String relativePath);
}
