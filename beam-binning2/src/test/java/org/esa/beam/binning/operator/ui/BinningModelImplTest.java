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

package org.esa.beam.binning.operator.ui;

import org.esa.beam.framework.datamodel.Product;
import org.junit.Test;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Thomas Storm
 */
public class BinningModelImplTest {

    @Test
    public void testSetGetProperty() throws Exception {
        final BinningModelImpl binningModel = new BinningModelImpl();
        binningModel.setProperty("key", new Float[]{2.0f, 3.0f});
        binningModel.setProperty("key2", new Integer[]{10, 20, 30});

        assertArrayEquals(new Product[0], binningModel.getSourceProducts());
        assertArrayEquals(new Float[]{2.0f, 3.0f}, (Float[])binningModel.getPropertyValue("key"));
        assertArrayEquals(new Integer[]{10, 20, 30}, (Integer[]) binningModel.getPropertyValue("key2"));
    }

    @Test
    public void testVariableConfigurationProperty() throws Exception {
        final BinningModel binningModel = new BinningModelImpl();
        assertArrayEquals(new VariableConfig[0], binningModel.getVariableConfigurations());

        final VariableConfig variableConfig = new VariableConfig("name", "name", null, 0.1, 0.2);
        binningModel.setProperty(BinningModel.PROPERTY_KEY_VARIABLE_CONFIGS,
                                 new VariableConfig[]{variableConfig});

        assertArrayEquals(new VariableConfig[]{variableConfig}, binningModel.getVariableConfigurations());
    }

    @Test
    public void testListening() throws Exception {
        final BinningModel binningModel = new BinningModelImpl();
        final MyPropertyChangeListener listener = new MyPropertyChangeListener();
        binningModel.addPropertyChangeListener(listener);
        
        binningModel.setProperty("key1", "value1");
        binningModel.setProperty("key2", "value2");
        
        assertEquals("value1", listener.targetMap.get("key1"));
        assertEquals("value2", listener.targetMap.get("key2"));
    }

    @Test
    public void testRegion() throws Exception {
    }

    private static class MyPropertyChangeListener implements PropertyChangeListener {

        Map<String, Object> targetMap = new HashMap<String, Object>();

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            targetMap.put(evt.getPropertyName(), evt.getNewValue());
        }
    }
}
