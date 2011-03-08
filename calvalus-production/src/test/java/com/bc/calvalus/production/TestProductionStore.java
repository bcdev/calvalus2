package com.bc.calvalus.production;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Test implementation of ProductionStore.
 */
public class TestProductionStore implements ProductionStore {
    private final ArrayList<Production> list = new ArrayList<Production>();

    @Override
    public void addProduction(Production production) {
        list.add(production);
    }

    @Override
    public void removeProduction(Production production) {
        list.remove(production);
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
    public void load() throws IOException {
    }

    @Override
    public void store() throws IOException {
    }
}
