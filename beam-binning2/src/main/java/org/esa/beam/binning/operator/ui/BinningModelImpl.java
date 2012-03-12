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
import com.bc.ceres.binding.ValidationException;
import com.bc.ceres.binding.accessors.DefaultPropertyAccessor;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;

/**
 * The model responsible for managing the binning parameters.
 *
 * @author Olaf Danne
 * @author Thomas Storm
 */
class BinningModelImpl implements BinningModel {

    private PropertySet propertySet;

    public BinningModelImpl() {
        this.propertySet = new PropertyContainer();
    }

    @Override
    public <T> T getProperty(String key) {
        final Property property = propertySet.getProperty(key);
        if(property != null) {
            return property.getValue();
        }
        return null;
    }

    @Override
    public Product[] getSourceProducts() throws IOException {
        final File[] files = getProperty(BinningModel.PROPERTY_KEY_SOURCE_PRODUCTS);
        if(files == null) {
            return new Product[0];
        }
        Product[] products = new Product[files.length];
        for (int i = 0; i < files.length; i++) {
            products[i] = ProductIO.readProduct(files[i]);
        }
        return products;
    }

    @Override
    public void setProperty(String key, Object value) throws ValidationException {
        final PropertyDescriptor descriptor;
        if(value == null) {
            descriptor = new PropertyDescriptor(key, Object.class);
        } else {
            descriptor = new PropertyDescriptor(key, value.getClass());
        }
        final Property property = new Property(descriptor, new DefaultPropertyAccessor());
        propertySet.addProperty(property);
        property.setValue(value);
        // todo -- remove this line
        System.out.println("set property: 'key = " + key + ", value = " + value + "'.");
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        propertySet.addPropertyChangeListener(propertyChangeListener);
    }
}
