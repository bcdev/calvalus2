package com.bc.calvalus.production.store;

import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;

/**
 * Persistence for productions.
 *
 * @author Norman
 */
public interface ProductionStore {
    void addProduction(Production production) throws ProductionException;

    void removeProduction(Production production) throws ProductionException;

    Production[] getProductions() throws ProductionException;

    Production getProduction(String productionId) throws ProductionException;

    void load() throws ProductionException;

    void store() throws ProductionException;

    /**
     * Indicates the service will no longer be used.
     * Invocation has no additional effect if already closed.
     * @throws Exception If any error occurs
     */
    void close() throws ProductionException;
}
