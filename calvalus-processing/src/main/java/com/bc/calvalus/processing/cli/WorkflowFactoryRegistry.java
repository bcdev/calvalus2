/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.cli;


import java.util.Collections;
import java.util.HashMap;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * A simple registry for {@link com.bc.calvalus.processing.cli.WorkflowFactory}s.
 *
 * @author MarcoZ
 * @author Norman
 */
public class WorkflowFactoryRegistry {
    private final HashMap<String, WorkflowFactory> map;

    private WorkflowFactoryRegistry() {
        map = new HashMap<String, WorkflowFactory>();
        ServiceLoader<WorkflowFactory> serviceLoader = ServiceLoader.load(WorkflowFactory.class);
        for (WorkflowFactory workflowFactory : serviceLoader) {
            map.put(workflowFactory.getName(), workflowFactory);
        }
    }

    public static WorkflowFactoryRegistry getInstance() {
        return Holder.instance;
    }

    public WorkflowFactory getWorkflowFactory(String name) {
        return map.get(name);
    }

    public Set<String> getNames() {
        return Collections.unmodifiableSet(map.keySet());
    }

    // Initialization-on-demand holder idiom
    private static class Holder {
        private static final WorkflowFactoryRegistry instance = new WorkflowFactoryRegistry();
    }
}
