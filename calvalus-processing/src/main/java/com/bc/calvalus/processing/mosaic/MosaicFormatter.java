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

import java.awt.Point;
import java.awt.Rectangle;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;


public class MosaicFormatter implements Configurable {

    private static final String PART_FILE_PREFIX = "part-r-";
    private Configuration jobConfig;

    @Override
    public void setConf(Configuration jobConfig) {
        this.jobConfig = jobConfig;
    }

    @Override
    public Configuration getConf() {
        return jobConfig;
    }

    public void processAllPartsToOneProduct() throws IOException {
        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(getConf());
        MosaicGrid mosaicGrid = new MosaicGrid();

        Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
        Rectangle productRegion = mosaicGrid.alignToTileGrid(geometryRegion);

        Product product = createProduct("mosaic-result", productRegion, algorithm.getOutputFeatures());
        CrsGeoCoding geoCoding = createCRS(productRegion, mosaicGrid.getPixelSize());
        product.setGeoCoding(geoCoding);

        ProductWriter productWriter = createProductWriter(product, "/tmp/mosaic_we_sr_4.nc");

        try {
            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/abc_we-lc-sr/");

            final FileStatus[] parts = getPartFiles(partsDir, FileSystem.get(jobConfig));
            for (FileStatus part : parts) {
                Path partFile = part.getPath();
                System.out.println(MessageFormat.format("reading and handling part {0}", partFile));
                handlePart(product, productWriter, partFile, mosaicGrid, productRegion);
            }
        } finally {
            productWriter.close();
        }
    }

    public void process5by5degreeProducts() throws IOException {
        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(getConf());
        MosaicGrid mosaicGrid = new MosaicGrid(180 / 5, 370 * 5);

//        Geometry regionGeometry = null;//JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
//        Rectangle geometryRegion = mosaicGrid.computeRegion(regionGeometry);
//        Rectangle globalRegion = mosaicGrid.alignToTileGrid(geometryRegion);

        Path partFile = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/abc_we-lc-sr/part-r-00001");

        Geometry partGeometry = null; //TODO get from part number
        Point[] tileIndices = mosaicGrid.getTileIndices(partGeometry);

        for (Point tile : tileIndices) {
            Rectangle productRegion = mosaicGrid.getTileRectangle(tile.x, tile.y);
            String productName = getTileProductName("mosaic", tile.x, tile.y);
            Product product = createProduct(productName, productRegion, algorithm.getOutputFeatures());
            CrsGeoCoding geoCoding = createCRS(productRegion, mosaicGrid.getPixelSize());
            product.setGeoCoding(geoCoding);
            ProductWriter productWriter = createProductWriter(product, productName + ".nc");//TODO nc
            try {
                handlePart(product, productWriter, partFile, mosaicGrid, productRegion);
            } finally {
                productWriter.close();
            }
        }
    }

    static String getTileProductName(String prefix, int tileX, int tileY) {
        return String.format("%s_v%02dh%02d", prefix, tileX, tileY);
    }

    private ProductWriter createProductWriter(Product product, String outputFileName) throws IOException {
        String outputFormat = "NetCDF-BEAM"; // TODO
//        String outputFormat = ProductIO.DEFAULT_FORMAT_NAME;

        ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter = new BufferedProductWriter(productWriter);
        productWriter.writeProductNodes(product, outputFileName);
        return productWriter;
    }

    private Product createProduct(String productName, Rectangle outputRegion, String[] outputFeatures) {
        final Product product = new Product(productName, "CALVALUS-L3", outputRegion.width, outputRegion.height);
        for (String outputFeature : outputFeatures) {
            Band band = product.addBand(outputFeature, ProductData.TYPE_FLOAT32);
            band.setNoDataValue(Float.NaN);
            band.setNoDataValueUsed(true);
        }
        //TODO
        //product.setStartTime(formatterConfig.getStartTime());
        //product.setEndTime(formatterConfig.getEndTime());
        return product;
    }

    private CrsGeoCoding createCRS(Rectangle outputRegion, double pixelSize) {
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
        return geoCoding;
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

    private void handlePart(Product product, ProductWriter productWriter, Path partFile, MosaicGrid mosaicGrid, Rectangle productRegion) throws IOException {
        FileSystem hdfs = FileSystem.get(jobConfig);
        SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, partFile, jobConfig);
        try {
            Band[] bands = product.getBands();
            TileIndexWritable key = new TileIndexWritable();
            TileDataWritable data = new TileDataWritable();
            while (reader.next(key)) {
                Rectangle tileRect = mosaicGrid.getTileRectangle(key.getTileX(), key.getTileY());
                tileRect = makeRelativeTo(tileRect, productRegion);
                if (productRegion.contains(tileRect)) {
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

    private Rectangle makeRelativeTo(Rectangle tileRect, Rectangle productRegion) {
        return new Rectangle(tileRect.x - productRegion.x, tileRect.y - productRegion.y, tileRect.width, tileRect.height);
    }

}
