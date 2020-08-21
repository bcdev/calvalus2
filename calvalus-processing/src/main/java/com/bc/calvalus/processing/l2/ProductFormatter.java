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

package com.bc.calvalus.processing.l2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.ceres.core.runtime.internal.DirScanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.util.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Methods used for formatting eo data products
 */
public class ProductFormatter {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final String outputFormat;
    private final String outputFilename;
    private final String productFilename;
    private final String outputCompression;
    private File tmpDir;

    public ProductFormatter(String productName, String outputFormat, String desiredOutputCompression) {
        String outputExtension = "";
        if (outputFormat.equals("BEAM-DIMAP")) {
            outputExtension = ".dim";
            if ("dir".equals(desiredOutputCompression)) {
                outputCompression = "dir2";
            } else {
                outputCompression = "zip";
            }
        } else if (outputFormat.equalsIgnoreCase("xcube-zarr")) {
            outputExtension = ".zarr";
            if ("zip".equals(desiredOutputCompression)) {
                outputCompression = "zip";
            } else {
                outputCompression = "dir2";
            }
        } else if (outputFormat.equals("NetCDF4-LC")) {
            outputExtension = ".nc";
            outputCompression = ""; // no further compression required
        }  else if (outputFormat.equals("NetCDF4-Fire")) {
            outputExtension = ".nc";
            outputCompression = ""; // no further compression required
        } else if (outputFormat.equals("NetCDF")) {
            outputExtension = ".nc";
            outputFormat = "NetCDF-BEAM"; // use NetCDF with BEAM extensions
            outputCompression = desiredOutputCompression;
        } else if (outputFormat.equals("NetCDF4")) {
            outputExtension = ".nc";
            outputCompression = ""; // no further compression required
            outputFormat = "NetCDF4-BEAM"; // use NetCDF with BEAM extensions
        } else if (outputFormat.equals("GeoTIFF")) {
            outputExtension = ".tif";
            outputCompression = desiredOutputCompression;
        } else if (outputFormat.equals("BigGeoTiff") || outputFormat.equals("GeoTIFF-BigTIFF")) {
            outputExtension = ".tif";
            outputCompression = "";
            outputFormat = "GeoTIFF-BigTIFF";
        } else if (outputFormat.equals("CSV")) {
            outputExtension = ".txt";
            outputCompression = "";
        } else if (outputFormat.equals("dir")){
            outputCompression = "dir";
            outputFormat = "NetCDF4-BEAM";
        } else {
            outputCompression = desiredOutputCompression;
        }
        // test if writer for output format exists
        ProductIOPlugInManager registry = ProductIOPlugInManager.getInstance();
        Iterator it = registry.getWriterPlugIns(outputFormat);
        if(it.hasNext()) {
            ProductWriterPlugIn plugIn = (ProductWriterPlugIn) it.next();
            if (outputExtension.isEmpty()) {
                // get output extension from writer
                String[] defaultFileExtensions = plugIn.getDefaultFileExtensions();
                if (defaultFileExtensions != null && defaultFileExtensions.length > 0) {
                    outputExtension = defaultFileExtensions[0];
                }
            }
        } else {
            throw new IllegalArgumentException("Unsupported output format: " + outputFormat);
        }

        if ("zip".equals(outputCompression)) {
            outputFilename = productName + ".zip";
        } else if ("gz".equals(outputCompression)) {
            outputFilename = productName + outputExtension + ".gz";
        } else {
            outputFilename = productName + outputExtension;
        }
        this.outputFormat = outputFormat;
        this.productFilename = productName + outputExtension;
    }

    public String getOutputFilename() {
        return outputFilename;
    }

    public String getProductFilename() {
        return productFilename;
    }

    public String getOutputFormat() {
        return outputFormat;
    }

    public String getOutputCompression() {
        return outputCompression;
    }

    public File createTemporaryProductFile() throws IOException {
        tmpDir = new File(System.getProperty("java.io.tmpdir"), "tmpProductDir");
        if (tmpDir.exists()) {
            FileUtils.deleteTree(tmpDir);
        }
        if (!tmpDir.mkdir()) {
            throw new IOException("Failed to create tmp directory: " + tmpDir.getAbsolutePath());
        }
        return new File(tmpDir, getProductFilename());
    }

    public void cleanupTempDir() {
        if (tmpDir != null) {
            FileUtils.deleteTree(tmpDir);
        }
    }

    public void compressToHDFS(TaskInputOutputContext<?, ?, ?, ?> context, File productFile) throws IOException {

        if ("zip".equals(outputCompression)) {
            LOG.info("Creating ZIP archive on HDFS.");
            OutputStream outputStream = createOutputStream(context, outputFilename);
            zip(tmpDir, outputStream, context);
        } else if ("gz".equals(outputCompression)) {
            LOG.info("Creating GZ file on HDFS.");
            InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
            OutputStream outputStream = createOutputStream(context, outputFilename);
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
            copyAndClose(inputStream, gzipOutputStream, context);
        } else if ("dir".equals(outputCompression)) {
            // currently used only for NetCDF4-BEAM products with IsinPlanetaryGrid
            LOG.info("Copying content of tmpDir to HDFS.");
            File[] files = tmpDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    LOG.info("Copying file " + file.getName() + " to HDFS.");
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(file));
                    OutputStream outputStream = createOutputStream(context, file.getName());
                    copyAndClose(inputStream, outputStream, context);
                }
            }
        } else if ("dir2".equals(outputCompression)) {
            LOG.info("Copying content of tmpDir to HDFS.");
            DirScanner dirScanner = new DirScanner(tmpDir, true, true);
            String[] entryPaths = dirScanner.scan();
            for (String entryPath : entryPaths) {
                File sourceFile = new File(tmpDir.getAbsolutePath() + File.separator + entryPath);
                InputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFile));
                OutputStream outputStream = createOutputStream(context, entryPath);
                copyAndClose(inputStream, outputStream, context);
            }
        } else {
            LOG.info("Copying file to HDFS.");
            InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
            OutputStream outputStream = createOutputStream(context, outputFilename);
            copyAndClose(inputStream, outputStream, context);
        }
        LOG.info("Finished writing to HDFS.");
    }

    public static void copyAndClose(InputStream inputStream, OutputStream outputStream,
                                    Progressable progressable) throws IOException {
        try {
            copy(inputStream, outputStream, progressable);
        } finally {
            try {
                inputStream.close();
            } finally {
                outputStream.close();
            }
        }
    }

    // copied from Staging
    public static void zip(File sourceDir, OutputStream outputStream, Progressable progressable) throws IOException {
        if (!sourceDir.exists()) {
            throw new FileNotFoundException(sourceDir.getPath());
        }

        // Important: First scan, ...
        DirScanner dirScanner = new DirScanner(sourceDir, true, true);
        String[] entryNames = dirScanner.scan();
        //            ... then create new file (avoid including the new ZIP in the ZIP!)
        ZipOutputStream zipOutputStream = new ZipOutputStream(new BufferedOutputStream(outputStream));
        zipOutputStream.setMethod(ZipEntry.DEFLATED);

        try {
            for (String entryName : entryNames) {
                ZipEntry zipEntry = new ZipEntry(entryName.replace('\\', '/'));

                File sourceFile = new File(sourceDir, entryName);
                FileInputStream inputStream = new FileInputStream(sourceFile);
                try {
                    zipOutputStream.putNextEntry(zipEntry);
                    copy(inputStream, zipOutputStream, progressable);
                    zipOutputStream.closeEntry();
                } finally {
                    inputStream.close();
                }
            }
        } finally {
            zipOutputStream.close();
        }
    }

    // copied from Staging
    public static void copy(InputStream inputStream, OutputStream outputStream, Progressable progressable) throws
                                                                                                           IOException {
        byte[] buffer = new byte[64 * 1024];
        while (true) {
            progressable.progress();
            int n = inputStream.read(buffer);
            if (n > 0) {
                outputStream.write(buffer, 0, n);
            } else if (n < buffer.length) {
                break;
            }
        }
    }

    public static OutputStream createOutputStream(TaskInputOutputContext<?, ?, ?, ?> context, String filename) throws
                                                                                                               IOException {
        Path workOutputPath;
        try {
            workOutputPath = FileOutputFormat.getWorkOutputPath(context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        Path workPath = new Path(workOutputPath, filename);
        FileSystem fileSystem = workPath.getFileSystem(context.getConfiguration());
        return fileSystem.create(workPath);
    }

}
