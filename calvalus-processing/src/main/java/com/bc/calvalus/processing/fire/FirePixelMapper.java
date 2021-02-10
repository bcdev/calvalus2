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

package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.hadoop.RasterStackWritable;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.binning.support.CrsGrid;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.common.SubsetOp;

import java.awt.Rectangle;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

public class FirePixelMapper extends Mapper<NullWritable, NullWritable, LongWritable, RasterStackWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numRowsGlobal = HadoopBinManager.getBinningConfig(conf).getNumRows();
        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        LOG.info(String.format("Input split: %s", fileSplit.toString()));

        File localOutputFile = CalvalusProductIO.copyFileToLocal(fileSplit.getPath(), conf);
        File[] untarredOutput = untar(localOutputFile, "(.*Classification.*|.*Uncertainty.*)");

        Product classificationProduct = ProductIO.readProduct(untarredOutput[0]);
        Product uncertaintyProduct = ProductIO.readProduct(untarredOutput[1]);

        LOG.info(String.format("classificationProduct: %s", classificationProduct.getName()));
        LOG.info(String.format("uncertaintyProduct: %s", uncertaintyProduct.getName()));

        Geometry continentalGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        Rectangle rectangle = SubsetOp.computePixelRegion(classificationProduct, continentalGeometry, 0);
        if (rectangle.isEmpty()) {
            LOG.info("No intersection between target area and input products - skipping.");
            return;
        }
        long binIndex = getBinIndex(classificationProduct, rectangle, numRowsGlobal);

        short[] classificationPixels = new short[rectangle.width * rectangle.height];
        byte[] uncertaintyPixels = new byte[rectangle.width * rectangle.height];

        readData(classificationProduct, rectangle, classificationPixels);
        readData(uncertaintyProduct, rectangle, uncertaintyPixels);

        RasterStackWritable rasterStackWritable = new RasterStackWritable(rectangle.width, rectangle.height, 2);
        rasterStackWritable.setBandType(0, RasterStackWritable.Type.SHORT);
        rasterStackWritable.setData(0, classificationPixels, RasterStackWritable.Type.SHORT);
        rasterStackWritable.setBandType(1, RasterStackWritable.Type.BYTE);
        rasterStackWritable.setData(1, uncertaintyPixels, RasterStackWritable.Type.BYTE);

        context.write(new LongWritable(binIndex), rasterStackWritable);

        LOG.info(String.format("Input %s: key %s, width %s, height %s", fileSplit.getPath().getName(), binIndex, rectangle.width, rectangle.height));
    }

    static long getBinIndex(Product classificationProduct, Rectangle rectangle, int numRowsGlobal) {
        GeoPos geoPos = new GeoPos();
        classificationProduct.getSceneGeoCoding().getGeoPos(new PixelPos(rectangle.x, rectangle.y), geoPos);
        CrsGrid crsGrid = new CrsGrid(numRowsGlobal, "EPSG:4326");
        return crsGrid.getBinIndex(geoPos.lat, geoPos.lon);
    }

    static void readData(Product uncertaintyProduct, Rectangle rectangle, byte[] uncertaintyPixels) throws IOException {
        Band classificationProductBand = uncertaintyProduct.getBand("band_1");
        ProductData.Byte buffer = new ProductData.Byte(uncertaintyPixels);
        classificationProductBand.readRasterData(rectangle.x, rectangle.y, rectangle.width, rectangle.height, buffer);
    }

    static void readData(Product classificationProduct, Rectangle rectangle, short[] classificationPixels) throws IOException {
        Band classificationProductBand = classificationProduct.getBand("band_1");
        ProductData.Short buffer = new ProductData.Short(classificationPixels);
        classificationProductBand.readRasterData(rectangle.x, rectangle.y, rectangle.width, rectangle.height, buffer);
    }

    private static File[] untar(File tarFile, String filterRegEx) {
        return untar(tarFile, filterRegEx, null);
    }

    private static File[] untar(File tarFile, String filterRegEx, List<String> newDirs) {
        List<File> result = new ArrayList<>();
        try (FileInputStream in = new FileInputStream(tarFile);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

            TarArchiveEntry entry;

            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    if (!new File(entry.getName()).exists()) {
                        boolean created = new File(entry.getName()).mkdirs();
                        if (!created) {
                            throw new IOException(String.format("Unable to create directory '%s' during extraction of contents of archive: '", entry.getName()));
                        }
                    }
                } else {
                    int count;
                    byte[] data = new byte[1024];
                    if (!entry.getName().matches(filterRegEx)) {
                        continue;
                    }
                    int lastIndex = entry.getName().lastIndexOf("/");
                    boolean created = new File(entry.getName().substring(0, lastIndex)).mkdirs();
                    if (created && newDirs != null) {
                        Collections.addAll(newDirs, entry.getName().substring(0, lastIndex).split("/"));
                    }
                    FileOutputStream fos = new FileOutputStream(entry.getName(), false);
                    try (BufferedOutputStream dest = new BufferedOutputStream(fos, 1024)) {
                        while ((count = tarIn.read(data, 0, 1024)) != -1) {
                            dest.write(data, 0, count);
                        }
                    }
                    result.add(new File(entry.getName()));
                }
            }

        } catch (IOException e) {
            throw new IllegalStateException("Unable to extract tar archive '" + tarFile + "'", e);
        }
        File[] untarredFiles = result.toArray(new File[0]);
        Arrays.sort(untarredFiles, Comparator.comparing(File::getName));
        return untarredFiles;
    }
}
