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
        final BinningModel binningModel = new BinningModelImpl();
        binningModel.setProperty("key", new Float[]{2.0f, 3.0f});
        binningModel.setProperty("key2", new int[]{10, 20, 30});

        assertArrayEquals(new Product[0], binningModel.getSourceProducts());
        assertArrayEquals(new Float[]{2.0f, 3.0f}, (Float[])binningModel.getProperty("key"));
        assertArrayEquals(new int[]{10, 20, 30}, (int[]) binningModel.getProperty("key2"));
    }

    @Test
    public void testListening() throws Exception {
        final BinningModel binningModel = new BinningModelImpl();
        final MyPropertyChangeListener listener = new MyPropertyChangeListener();
        binningModel.addPropertyChangeListener(listener);
        
        binningModel.setProperty("key1", "value1");
        binningModel.setProperty("key2", "value2");
        
        assertEquals("value1", listener.map.get("key1"));
        assertEquals("value2", listener.map.get("key2"));
    }

    private static class MyPropertyChangeListener implements PropertyChangeListener {

        Map<String, Object> map = new HashMap<String, Object>();

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            map.put(evt.getPropertyName(), evt.getNewValue());
        }
    }
}
