package com.bc.calvalus.production;

import com.bc.calvalus.catalogue.ProductSet;

/**
 * The interface to the Calvalus production service.
 *
 * @author Norman
 */
public interface ProductionService {

    /**
     * Gets all known product sets.
     * TODO - Check: move to CatalogueService?
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
    Processor[] getProcessors(String filter) throws ProductionException;

    /**
     * Gets all known productions.
     *
     * @param filter A filter expression (not yet used).
     * @return The productions.
     * @throws ProductionException If a service error occurred.
     */
    Production[] getProductions(String filter) throws ProductionException;

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
    void cancelProductions(String[] productionIds) throws ProductionException;

    /**
     * Requests deletion of productions with given IDs.
     * @param productionIds The production IDs.
     * @throws ProductionException If a service error occurred.
     */
    void deleteProductions(String[] productionIds) throws ProductionException;
 }
