package com.bc.calvalus.production.store;

import com.bc.calvalus.production.Production;
import org.junit.Ignore;

import java.util.ArrayList;

/**
 * Test implementation of ProductionStore.
 */
@Ignore
public class TestProductionStore implements ProductionStore {
    private final ArrayList<Production> list = new ArrayList<Production>();
    boolean closed;

    @Override
    public void addProduction(Production production) {
        list.add(production);
    }

    @Override
    public void removeProduction(String productionId) {
        Production production = getProduction(productionId);
        if (production != null) {
            list.remove(production);
        }
    }

    @Override
    public Production[] getProductions() {
        return list.toArray(new Production[list.size()]);
    }

    @Override
    public Production getProduction(String productionId) {
        for (Production production : list) {
            if (productionId.equals(production.getId())) {
                return production;
            }
        }
        return null;
    }

    @Override
    public void update() {
    }

    @Override
    public void persist() {
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}
