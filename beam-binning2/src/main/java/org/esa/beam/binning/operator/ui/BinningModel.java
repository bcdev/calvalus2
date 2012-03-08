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

import com.bc.ceres.binding.Property;
import com.bc.ceres.binding.PropertyContainer;
import com.bc.ceres.binding.PropertyDescriptor;
import com.bc.ceres.binding.PropertySet;
import com.bc.ceres.binding.accessors.DefaultPropertyAccessor;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * TODO fill out or delete
 *
 * @author Thomas Storm
 */
public class BinningModel {

    private File[] files;
    private PropertySet propertySet;

    public BinningModel() {
        this.propertySet = new PropertyContainer();
    }

    public PropertySet getPropertySet() {
        return propertySet;
    }

    public void setSourceProducts(File[] files) {
        this.files = files;
    }

    public Map<String, Object> getParameters() {
        return null;
    }

    public Product[] getSourceProducts() throws IOException {
        Product[] products = new Product[files.length];
        for (int i = 0; i < files.length; i++) {
            products[i] = ProductIO.readProduct(files[i]);
        }
        return products;
    }

    public void setProperty(String key, Object value) {
        final PropertyDescriptor descriptor;
        if(value == null) {
            descriptor = new PropertyDescriptor(key, Object.class);
        } else {
            descriptor = new PropertyDescriptor(key, value.getClass());
        }
        final Property property = new Property(descriptor, new DefaultPropertyAccessor());
        propertySet.addProperty(property);
        System.out.println("set property: 'key = " + key + ", value = " + value + "'.");
    }
}
