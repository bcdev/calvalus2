package com.bc.calvalus.production.store;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;

/**
 * A store used to keep productions.
 *
 * @author Norman
 */
public interface ProductionStore {

    /**
     * Adds a production to the store.
     * The addition and all further modifications of the given {@code production} object
     * are not persistent until {@link #persist()} is called.
     *
     * @param production The production.
     */
    void addProduction(Production production);

    /**
     * Removes a production from the store.
     * The removal of the given {@code production} object
     * is not persistent until {@link #persist()} is called.
     *
     * @param productionId The production ID.
     */
    void removeProduction(String productionId);

    /**
     * Gets all productions currently kept in this store.
     * The state of the
     * returned {@code Production} objects may change
     * after {@link #update()} has been called on this store.
     *
     * @return The array of productions.
     */
    Production[] getProductions();

    /**
     * Gets the production with the specified ID.
     *
     * @param productionId The production ID.
     * @return The production, or {@code null} if no such exists.
     */
    Production getProduction(String productionId);

    /**
     * Updates this store by changes received from the underlying database.
     *
     * @throws ProductionException If an eror occured.
     */
    void update() throws ProductionException;

    /**
     * Makes the current state of this store persistent in an underlying database.
     *
     * @throws ProductionException If an eror occured.
     */
    void persist() throws ProductionException;

    /**
     * Closes the connection to the underlying database (if any).
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     *
     * @throws ProductionException If the store could not be closed.
     */
    void close() throws ProductionException;
}
