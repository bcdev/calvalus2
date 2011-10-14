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

import com.bc.calvalus.processing.JobUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.CrsGeoCoding;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.awt.Rectangle;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;

/**
 * Created by IntelliJ IDEA.
 * User: marcoz
 * Date: 12.10.11
 * Time: 16:25
 * To change this template use File | Settings | File Templates.
 */
public class MosaicFormatter {

    private static Product createProduct(String outputName, Rectangle outputRegion, double pixelSize) {

        CrsGeoCoding geoCoding;
        try {
            geoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84,
                                         outputRegion.width,
                                         outputRegion.height,
                                         -180.0 + pixelSize * outputRegion.x,
                                         90.0 - pixelSize * outputRegion.y,
                                         pixelSize,
                                         pixelSize,
                                         0.0, 0.0);
        } catch (FactoryException e) {
            throw new IllegalStateException(e);
        } catch (TransformException e) {
            throw new IllegalStateException(e);
        }
        final Product product = new Product(outputName, "CALVALUS-L3", outputRegion.width, outputRegion.height);
        product.setGeoCoding(geoCoding);
        //TODO
//        product.setStartTime(formatterConfig.getStartTime());
//        product.setEndTime(formatterConfig.getEndTime());
        return product;
    }

    private static final String PART_FILE_PREFIX = "part-r-";

    public static void main(String[] args) throws IOException {

//        Geometry geometry = JobUtils.createGeometry("polygon((-2 46, -2 40, 1 40, 1 46, -2 46))");
        Geometry geometry = JobUtils.createGeometry("polygon((-2 43, -2 40, 1 40, 1 43, -2 43))");

        MosaicGrid mosaicGrid = new MosaicGrid();
        Rectangle geometryRegion = mosaicGrid.computeRegion(geometry);
        System.out.println("geometryRegion = " + geometryRegion);
        Rectangle gridRegion = mosaicGrid.alignToTileGrid(geometryRegion);
        System.out.println("gridRegion = " + gridRegion);

        Product product = createProduct("test", gridRegion, mosaicGrid.getPixelSize());
        Band[] bands = new Band[3];
        for (int i = 0; i < bands.length; i++) {
            bands[i] = product.addBand("band_" + (i), ProductData.TYPE_FLOAT32);
        }

        String outputFormat = "NetCDF-BEAM"; // TODO
        final ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter.writeProductNodes(product, "/tmp/mosaic.nc");
        try {


            Configuration configuration = new Configuration();
            configuration.set("fs.default.name", "hdfs://cvmaster00:9000");

            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-sdr-oneyear/test-L3-1/");
            final FileSystem hdfs = partsDir.getFileSystem(configuration);
            final FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return path.getName().startsWith(PART_FILE_PREFIX);
                }
            });
            System.out.println(MessageFormat.format("collecting {0} parts", parts.length));
            Arrays.sort(parts);

            Rectangle productBounds = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
            for (FileStatus part : parts) {
                Path partFile = part.getPath();
                SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, configuration);
                System.out.println(MessageFormat.format("reading and handling part {0}", partFile));
                try {
                    TileIndexWritable key = new TileIndexWritable();
                    TileDataWritable data = new TileDataWritable();
                    while (reader.next(key)) {
                        Rectangle tileRect = mosaicGrid.getTileRect(key.getTileX(), key.getTileY(), gridRegion);
                        if (productBounds.contains(tileRect)) {
                            reader.getCurrentValue(data);
                            float[][] samples = data.getSamples();
                            for (int i = 0; i < bands.length; i++) {
                                ProductData productData = ProductData.createInstance(samples[i]);
                                productWriter.writeBandRasterData(bands[i], tileRect.x, tileRect.y, tileRect.width, tileRect.height, productData, ProgressMonitor.NULL);
                            }
                        }
                    }
                } finally {
                    reader.close();
                }
            }

        } finally {
            productWriter.close();
        }
    }
}
