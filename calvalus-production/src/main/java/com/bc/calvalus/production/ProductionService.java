package com.bc.calvalus.production;

import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor;

import java.io.IOException;

/**
 * The interface to the Calvalus production service.
 *
 * @author Norman
 */
public interface ProductionService {

    /**
     * Gets all known product sets.
     *
     * @param filter A filter expression (not yet used).
     * @return The product sets.
     * @throws ProductionException If a service error occurred.
     */
    ProductSet[] getProductSets(String filter) throws ProductionException;

    /**
     * Gets all known processors.
     *
     * @param filter A filter expression (not yet used).
     * @return The processors.
     * @throws ProductionException If a service error occurred.
     */
    ProcessorDescriptor[] getProcessors(String filter) throws ProductionException;

    /**
     * Gets all known productions.
     *
     * @param filter A filter expression (not yet used).
     * @return The productions.
     * @throws ProductionException If a service error occurred.
     */
    Production[] getProductions(String filter) throws ProductionException;

    /**
     * Gets the production for the given ID.
     *
     * @param id The production ID.
     * @return The production, or {@code null} if none with the given ID was found.
     * @throws ProductionException If a service error occurred.
     */
    Production getProduction(String id) throws ProductionException;

    /**
     * Orders a new productions.
     *
     * @param request The request.
     * @return The response.
     * @throws ProductionException If a service error occurred.
     */
    ProductionResponse orderProduction(ProductionRequest request) throws ProductionException;

    /**
     * Requests cancellation of productions with given IDs.
     * @param productionIds The production IDs.
     * @throws ProductionException If a service error occurred.
     */
    void cancelProductions(String ... productionIds) throws ProductionException;

    /**
     * Requests deletion of productions with given IDs.
     * @param productionIds The production IDs.
     * @throws ProductionException If a service error occurred.
     */
    void deleteProductions(String ... productionIds) throws ProductionException;

    /**
     * Requests stating of productions with given IDs.
     * @param productionIds The production IDs.
     * @throws ProductionException If a service error occurred.
     */
    void stageProductions(String ... productionIds) throws ProductionException;

    // todo - actually the service shall update itself on a regular basis (nf)
    /**
     * A request to retrieve and update the status of all workflows.
     */
    void updateStatuses();

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     */
    void close() throws IOException;
}
