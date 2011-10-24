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
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.ProgressMonitorWrapper;
import com.bc.ceres.core.runtime.internal.DirScanner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.esa.beam.util.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

    public ProductFormatter(String productName, String outputFormat, String outputCompression) {
        this.outputCompression = outputCompression;
        String outputExtension;
        if (outputFormat.equals("BEAM-DIMAP")) {
            outputExtension = ".dim";
        } else if (outputFormat.equals("NetCDF")) {
            outputExtension = ".nc";
            outputFormat = "NetCDF-BEAM"; // use NetCDF with BEAM extensions
        } else if (outputFormat.equals("GeoTIFF")) {
            outputExtension = ".tif";
        } else {
            throw new IllegalArgumentException("Unsupported output format: " + outputFormat);
        }
        this.outputFormat = outputFormat;


        if ("zip".equals(outputCompression)) {
            outputFilename = productName + ".zip";
        } else if ("gz".equals(outputCompression)) {
            outputFilename = productName + outputExtension + ".gz";
        } else {
            outputFilename = productName + outputExtension;
        }

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

    public void compressToHDFS(TaskInputOutputContext<?, ?, ?, ?> context, File productFile) throws IOException, InterruptedException {
        OutputStream outputStream = createOutputStream(context, outputFilename);
        if ("zip".equals(outputCompression)) {
            LOG.info("Creating ZIP archive on HDFS.");
            zip(tmpDir, outputStream, context);
        } else if ("gz".equals(outputCompression)) {
            LOG.info("Creating GZ file on HDFS.");
            InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
            copyAndClose(inputStream, gzipOutputStream, context);
        } else {
            LOG.info("Copying file to HDFS.");
            InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
            copyAndClose(inputStream, outputStream, context);
        }
        LOG.info("Finished writing to HDFS.");
    }

    static void copyAndClose(InputStream inputStream, OutputStream outputStream, Progressable progressable) throws IOException {
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
    static void zip(File sourceDir, OutputStream outputStream, Progressable progressable) throws IOException {
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
    static void copy(InputStream inputStream, OutputStream outputStream, Progressable progressable) throws IOException {
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

    static OutputStream createOutputStream(TaskInputOutputContext<?, ?, ?, ?> context, String filename) throws IOException, InterruptedException {
        Path workPath = new Path(FileOutputFormat.getWorkOutputPath(context), filename);
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        return fileSystem.create(workPath, (short) 1);
    }


    public static class ProgressMonitorAdapter extends ProgressMonitorWrapper {

        private final Progressable progressable;

        public ProgressMonitorAdapter(Progressable progressable) {
            super(ProgressMonitor.NULL);
            this.progressable = progressable;
        }

        @Override
        public void internalWorked(double work) {
            progressable.progress();
        }
    }
}
