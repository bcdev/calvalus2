/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;


import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.esa.beam.dataio.envisat.EnvisatProductReaderPlugIn;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.util.SystemUtils;

import javax.imageio.stream.ImageInputStream;
import javax.media.jai.JAI;
import java.io.IOException;
import java.text.MessageFormat;

public class BeamProductHandler {
        //TODO make this an option
    private static final int TILE_HEIGHT = 128;
    private static final int TILE_CACHE_SIZE_M = 512;  // 512 MB

    static void init() {
        SystemUtils.init3rdPartyLibs(BeamProductHandler.class.getClassLoader());
        JAI.enableDefaultTileCache();
        JAI.getDefaultInstance().getTileCache().setMemoryCapacity(TILE_CACHE_SIZE_M * 1024 * 1024);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
    }

    int getTileHeight() {
        return TILE_HEIGHT;
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param inputPath         The input path
     * @param configuration the configuration
     * @return The product
     * @throws java.io.IOException
     */
    Product readProduct(Path inputPath, Configuration configuration) throws IOException {
        final FileSystem fs = inputPath.getFileSystem(configuration);
        final FileStatus status = fs.getFileStatus(inputPath);
        final FSDataInputStream in = fs.open(inputPath);
        final ImageInputStream imageInputStream = new FSImageInputStream(in, status.getLen());
        System.setProperty("beam.envisat.tileHeight", Integer.toString(getTileHeight()));
        final EnvisatProductReaderPlugIn plugIn = new EnvisatProductReaderPlugIn();
        final ProductReader productReader = plugIn.createReaderInstance();
        Product product = productReader.readProductNodes(imageInputStream, null);
        if (product == null) {
            throw new IllegalStateException(MessageFormat.format("No reader found for product {0}", inputPath));
        }
        return product;
    }

}
