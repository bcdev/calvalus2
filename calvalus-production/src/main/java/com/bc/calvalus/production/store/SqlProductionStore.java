package com.bc.calvalus.production.store;

import com.bc.calvalus.production.Production;

import java.io.IOException;

/**
 * An SQL-based database for productions.
 *
 * @author Norman
 */
public class SqlProductionStore implements ProductionStore {

    @Override
    public synchronized void addProduction(Production production) {
    }

    @Override
    public synchronized void removeProduction(Production production) {
    }

    @Override
    public synchronized Production[] getProductions() {
        return null;
    }

    @Override
    public synchronized Production getProduction(String productionId) {
        return null;
    }

    @Override
    public synchronized void load() throws IOException {
    }

    @Override
    public synchronized void store() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
}
