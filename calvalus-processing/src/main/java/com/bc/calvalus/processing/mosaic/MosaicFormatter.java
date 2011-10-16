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

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.ceres.core.ProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
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


public class MosaicFormatter implements Configurable {

    private static final String PART_FILE_PREFIX = "part-r-";
    private MosaicGrid mosaicGrid;
    private Rectangle gridRegion;
    private Configuration jobConfig;

    @Override
    public void setConf(Configuration jobConfig) {
        this.jobConfig = jobConfig;
        mosaicGrid = new MosaicGrid();
        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
        gridRegion = mosaicGrid.alignToTileGrid(geometryRegion);
    }

    @Override
    public Configuration getConf() {
        return jobConfig;
    }

    public void run() throws IOException {
        Product product = createProduct("mosaic-result", gridRegion, mosaicGrid.getPixelSize());

        String outputFormat = "NetCDF-BEAM"; // TODO
        final ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter.writeProductNodes(product, "/tmp/mosaic.nc");

        try {
            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-sdr-oneyear/test-L3-1/");

            final FileStatus[] parts = getPartFiles(partsDir, FileSystem.get(jobConfig));
            for (FileStatus part : parts) {
                Path partFile = part.getPath();
                System.out.println(MessageFormat.format("reading and handling part {0}", partFile));
                handlePart(product, productWriter, partFile);
            }
        } finally {
            productWriter.close();
        }
    }

    private Product createProduct(String outputName, Rectangle outputRegion, double pixelSize) {
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
        for (int i = 0; i < 42; i++) {
            product.addBand("band_" + (i), ProductData.TYPE_FLOAT32);
        }

        //TODO
        //product.setStartTime(formatterConfig.getStartTime());
        //product.setEndTime(formatterConfig.getEndTime());
        return product;
    }

    private static FileStatus[] getPartFiles(Path partsDir, FileSystem hdfs) throws IOException {
        final FileStatus[] parts = hdfs.listStatus(partsDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(PART_FILE_PREFIX);
            }
        });
        System.out.println(MessageFormat.format("collecting {0} parts", parts.length));
        Arrays.sort(parts);
        return parts;
    }

    private void handlePart(Product product, ProductWriter productWriter, Path partFile) throws IOException {
        FileSystem hdfs = FileSystem.get(jobConfig);
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, jobConfig);
        try {
            Rectangle productBounds = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
            Band[] bands = product.getBands();
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
}
