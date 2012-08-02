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

package com.bc.calvalus.processing.mosaic;

import org.apache.hadoop.conf.Configuration;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;

/**
 * A factory for creating the final mosaic product.
 *
 * @author MarcoZ
 */
public interface MosaicProductFactory {

    Product createProduct(String productName, Rectangle rect);
    
    Product createProduct(Configuration configuration, int x, int y, Rectangle productRect);

    String getTileProductName(String outputNameFormat, int x, int y);
}
