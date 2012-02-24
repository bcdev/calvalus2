/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.production.store;

import com.bc.calvalus.production.Production;

import java.util.ArrayList;

/**
 * An in memory implementation of ProductionStore.
 */
public class MemoryProductionStore implements ProductionStore {
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
