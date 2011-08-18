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

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.beam.MerisProductTestRunner;
import com.bc.ceres.glevel.MultiLevelImage;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

import static org.junit.Assert.assertTrue;

@RunWith(MerisProductTestRunner.class)
public class ProductSlicingTest {

    @Test
    public void testThatProductCanBeTiledInSlices() throws IOException {
        File testproductFile = MerisProductTestRunner.getTestProductFile();

        System.setProperty("beam.reader.tileHeight", "64");
        System.setProperty("beam.reader.tileWidth", "*");
        ProductReader productReader = ProductIO.getProductReader("ENVISAT");

        Product sourceProduct = productReader.readProductNodes(testproductFile, null);
        Band band = sourceProduct.getBand("radiance_13");
        assertThatImageIsSliced(sourceProduct, band.getSourceImage());
        assertThatImageIsSliced(sourceProduct, band.getValidMaskImage());
        assertThatImageIsSliced(sourceProduct, band.getGeophysicalImage());
    }

    private void assertThatImageIsSliced(Product product, MultiLevelImage image) {
        int tileWidth = image.getTileWidth();
        int sceneRasterWidth = product.getSceneRasterWidth();
        String msg = MessageFormat.format("Product not sliced: image.tileSize = {0}x{1}, product.sceneRasterSize = {2}x{3}",
                                          tileWidth, image.getTileHeight(), sceneRasterWidth, product.getSceneRasterHeight());
        assertTrue(msg, tileWidth == sceneRasterWidth);
    }

}
