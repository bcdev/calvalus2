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

package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.util.ProductUtils;
import org.esa.snap.dataio.bigtiff.BigGeoTiffProductWriterPlugIn;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * The fire formatting pixel mapper.
 *
 * @author thomas
 * @author marcop
 */
public class PixelMergeMapper extends Mapper<Text, FileSplit, Text, PixelCell> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        PixelProductArea area = PixelProductArea.valueOf(context.getConfiguration().get("area"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        File[] variableFiles = new File[3];

        for (int i = 0; i < paths.length; i++) {
            variableFiles[i] = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
        }

        Product inputVar1 = ProductIO.readProduct(variableFiles[0]);
        Product inputVar2 = ProductIO.readProduct(variableFiles[1]);
        Product inputVar3 = ProductIO.readProduct(variableFiles[2]);

        String baseFilename = createBaseFilename(year, month, area);
        Product result = new Product(baseFilename, "fire-cci-pixel-product", inputVar1.getSceneRasterWidth(), inputVar1.getSceneRasterHeight());
        ProductUtils.copyGeoCoding(inputVar1, result);
        ProductUtils.copyBand(inputVar1.getBandAt(0).getName(), inputVar1, result, true);
        ProductUtils.copyBand(inputVar2.getBandAt(0).getName(), inputVar2, result, true);
        ProductUtils.copyBand(inputVar3.getBandAt(0).getName(), inputVar3, result, true);

        CalvalusLogger.getLogger().info("Writing final product...");
        ProductIO.writeProduct(result, baseFilename + ".tif", BigGeoTiffProductWriterPlugIn.FORMAT_NAME);
        CalvalusLogger.getLogger().info("...done. Creating zip of final product...");
        String zipFilename = baseFilename + ".tar.gz";
        createTarGZ(baseFilename + ".tif", zipFilename);

        result.dispose();
        String outputDir = context.getConfiguration().get("calvalus.output.dir");
        Path path = new Path(outputDir + "/" + zipFilename);
        CalvalusLogger.getLogger().info(String.format("...done. Copying final product to %s...", path.toString()));
        FileSystem fs = path.getFileSystem(context.getConfiguration());
        FileUtil.copy(new File(zipFilename), fs, path, false, context.getConfiguration());
        CalvalusLogger.getLogger().info("...done.");
    }

    static String createBaseFilename(String year, String month, PixelProductArea area) {
        return String.format("%s%s01-ESACCI-L3S_FIRE-BA-MERIS-AREA_%d-v02.0-fv04.0", year, month, area.index);
    }

    private static void createTarGZ(String filePath, String outputPath) throws IOException {
        try (OutputStream fOut = new FileOutputStream(new File(outputPath));
             OutputStream bOut = new BufferedOutputStream(fOut);
             OutputStream gzOut = new GzipCompressorOutputStream(bOut);
             TarArchiveOutputStream tOut = new TarArchiveOutputStream(gzOut)) {
            addFileToTarGz(tOut, filePath, "");
        }
    }

    private static void addFileToTarGz(TarArchiveOutputStream tOut, String path, String base)
            throws IOException {
        File f = new File(path);
        String entryName = base + f.getName();
        TarArchiveEntry tarEntry = new TarArchiveEntry(f, entryName);
        tOut.putArchiveEntry(tarEntry);

        if (f.isFile()) {
            IOUtils.copy(new FileInputStream(f), tOut);
            tOut.closeArchiveEntry();
        } else {
            tOut.closeArchiveEntry();
            File[] children = f.listFiles();
            if (children != null) {
                for (File child : children) {
                    addFileToTarGz(tOut, child.getAbsolutePath(), entryName + "/");
                }
            }
        }
    }

}
