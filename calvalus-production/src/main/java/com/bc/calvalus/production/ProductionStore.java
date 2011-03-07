package com.bc.calvalus.production;

import com.bc.calvalus.production.Production;

import java.io.IOException;

/**
 * Persistence for productions.
 *
 * @author Norman
 */
public interface ProductionStore {
    void addProduction(Production production);

    void removeProduction(Production production);

    Production[] getProductions();

    Production getProduction(String productionId);

    void load() throws IOException;

    void store() throws IOException;
}
