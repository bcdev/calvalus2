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

import com.bc.ceres.binding.ValidationException;
import org.esa.beam.framework.datamodel.Product;

import java.beans.PropertyChangeListener;
import java.io.IOException;

/**
 * The model responsible for managing the binning parameters.
 *
 * @author Thomas Storm
 */
public interface BinningModel {

    String PROPERTY_KEY_SOURCE_PRODUCTS = "sourceProducts";
    String PROPERTY_KEY_CRS = "crs";
    String PROPERTY_KEY_VARIABLE_CONFIGS = "variableConfigs";

    /**
     * Returns the value of the property given by the key; <code>null</code> if it does not exist.
     * @param key the property key
     * @param <T> the property type
     * @return the property value or <code>null</code>.
     */
    <T> T getProperty(String key);

    Product[] getSourceProducts() throws IOException;

    void setProperty(String key, Object value) throws ValidationException;

    void addPropertyChangeListener(PropertyChangeListener propertyChangeListener);
}
