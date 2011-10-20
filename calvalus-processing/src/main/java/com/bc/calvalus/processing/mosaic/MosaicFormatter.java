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

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://cvmaster00:9000");

        configuration.set("calvalus.l3.parameters", "<parameters>\n" +
                "                    <numRows>66792</numRows>\n" +
                "                     <maskExpr>status == 1 or status == 3 or status == 4 or status == 5</maskExpr>\n" +
                "                    <variables>\n" +
                "\t\t              <variable><name>status</name></variable>\n" +
                "            \t\t\t<variable><name>sdr_1</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_2</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_3</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_4</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_5</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_6</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_7</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_8</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_9</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_10</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_11</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_12</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_13</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_14</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_15</name></variable>\n" +
                "\t\t\t            <variable><name>ndvi</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_1</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_2</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_3</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_4</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_5</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_6</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_7</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_8</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_9</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_10</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_11</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_12</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_13</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_14</name></variable>\n" +
                "\t\t\t            <variable><name>sdr_error_15</name></variable>\n" +
                "                    </variables>" +
                "<aggregators>\n" +
                "                        <aggregator>\n" +
                "                            <type>com.bc.calvalus.processing.mosaic.LCMosaicAlgorithm</type>\n" +
                "                        </aggregator>\n" +
                "                    </aggregators>                    \n" +
                "\t\t  </parameters>");

//configuration.set("calvalus.l3.parameters", "<parameters>\n" +
//                "                    <numRows>66792</numRows>\n" +
//                "                     <maskExpr>status == 1</maskExpr>\n" +
//                "                    <variables>\n" +
//                "\t\t              <variable><name>status</name></variable>\n" +
//                "\t\t\t            <variable><name>sdr_8</name></variable>\n" +
//                "                    </variables>" +
//                "<aggregators>\n" +
//                "                        <aggregator>\n" +
//                "                            <type>com.bc.calvalus.processing.mosaic.LcSDR8MosaicAlgorithm</type>\n" +
//                "                        </aggregator>\n" +
//                "                    </aggregators>                    \n" +
//                "\t\t  </parameters>");

//        configuration.set("calvalus.regionGeometry", "polygon((-2 46, -2 43, 1 43, 1 46, -2 46))");
        configuration.set("calvalus.regionGeometry", "POLYGON ((-7 54, -7 39, 6 39, 6 54, -7 54))");

        MosaicFormatter mosaicFormatter = new MosaicFormatter();
        mosaicFormatter.setConf(configuration);
        mosaicFormatter.run();
    }

    public void run() throws IOException {
        MosaicAlgorithm algorithm = MosaicUtils.createAlgorithm(getConf());
        Product product = createProduct("mosaic-result", algorithm, gridRegion, mosaicGrid.getPixelSize());

        String outputFormat = "NetCDF-BEAM"; // TODO
//        String outputFormat = ProductIO.DEFAULT_FORMAT_NAME;
        ProductWriter productWriter = ProductIO.getProductWriter(outputFormat);
        if (productWriter == null) {
            throw new IllegalArgumentException("No writer found for output format " + outputFormat);
        }
        productWriter = new BufferedProductWriter(productWriter);
        productWriter.writeProductNodes(product, "/tmp/mosaic_we_sr_4.nc");


        try {
//            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/abc_7-lc-sdr8Mean/");
            Path partsDir = new Path("hdfs://cvmaster00:9000/calvalus/outputs/lc-production/abc_we-lc-sr/");

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

    private Product createProduct(String outputName, MosaicAlgorithm algorithm, Rectangle outputRegion, double pixelSize) {
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

        String[] outputFeatures = algorithm.getOutputFeatures();
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
                Rectangle tileRect = createTileRectangle(key);
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

    private Rectangle createTileRectangle(TileIndexWritable key) {
        final int tileSize = mosaicGrid.getTileSize();
        return new Rectangle(key.getTileX() * tileSize - gridRegion.x,
                             key.getTileY() * tileSize - gridRegion.y,
                             tileSize, tileSize);
    }
}
