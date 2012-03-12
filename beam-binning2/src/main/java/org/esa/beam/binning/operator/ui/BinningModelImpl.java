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
import com.bc.ceres.swing.binding.BindingContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.ui.BoundsInputPanel;

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
    private BindingContext bindingContext;

    public BinningModelImpl() {
        propertySet = new PropertyContainer();
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_EAST_BOUND, Float.class));
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_NORTH_BOUND, Float.class));
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_WEST_BOUND, Float.class));
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_SOUTH_BOUND, Float.class));
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_PIXEL_SIZE_X, Float.class));
        propertySet.addProperty(createProperty(BoundsInputPanel.PROPERTY_PIXEL_SIZE_Y, Float.class));
        propertySet.addProperty(createProperty(BinningModel.PROPERTY_KEY_ENABLE, Boolean.class));
        propertySet.addProperty(createProperty(BinningModel.PROPERTY_KEY_GLOBAL, Boolean.class));
        propertySet.addProperty(createProperty(BinningModel.PROPERTY_KEY_COMPUTE_REGION, Boolean.class));
        propertySet.setDefaultValues();
    }

    private Property createProperty(String name, Class type) {
        final DefaultPropertyAccessor defaultAccessor = new DefaultPropertyAccessor();
        final PropertyDescriptor descriptor = new PropertyDescriptor(name, type);
        return new Property(descriptor, defaultAccessor);
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
    public BinningParametersPanel.VariableConfig[] getVariableConfigurations() {
        BinningParametersPanel.VariableConfig[] variableConfigs = getProperty(PROPERTY_KEY_VARIABLE_CONFIGS);
        if(variableConfigs == null) {
            variableConfigs = new BinningParametersPanel.VariableConfig[0];
        }
        return variableConfigs;
    }

    @Override
    public Region getRegion() {
        final Region wkt = Region.WKT;
        wkt.setWkt("");
        return wkt;
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

    @Override
    public BindingContext getBindingContext() {
        if(bindingContext == null) {
            bindingContext = new BindingContext(propertySet);
        }
        return bindingContext;
    }

    @SuppressWarnings("unchecked")
    <T> T getProperty(String key) {
        final Property property = propertySet.getProperty(key);
        if(property != null) {
            return (T)property.getValue();
        }
        return null;
    }
}
