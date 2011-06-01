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

import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class WorkflowFactoryRegistryTest {

    private WorkflowFactoryRegistry registry;

    @Before
    public void setUp() throws Exception {
        registry = WorkflowFactoryRegistry.getInstance();
    }

    @Test
    public void testInit() throws Exception {
        assertNotNull(registry);
    }

    @Test
    public void testGetNames() throws Exception {
        Set<String> names = registry.getNames();
        assertNotNull(names);
        assertEquals(2, names.size());
        assertTrue(names.contains("l2"));
        assertTrue(names.contains("l3"));
    }

    @Test
    public void testGetWorkflowFactory() throws Exception {
        WorkflowFactory workflowFactory = registry.getWorkflowFactory("");
        assertNull(workflowFactory);

        WorkflowFactory l2a = registry.getWorkflowFactory("l2");
        WorkflowFactory l2b = registry.getWorkflowFactory("l2");
        assertNotNull(l2a);
        assertSame(l2a, l2b);
    }
}
