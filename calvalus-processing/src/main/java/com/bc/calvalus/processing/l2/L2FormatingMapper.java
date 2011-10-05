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
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.ProgressMonitorWrapper;
import com.bc.ceres.core.runtime.internal.DirScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
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
 * A mapper which converts L2 products from the
 * (internal) SequenceFiles into different BAM product formats.
 */
public class L2FormatingMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException, ProcessorException {
        Configuration jobConfig = context.getConfiguration();
        ProductFactory productFactory = new ProductFactory(jobConfig);

        final FileSplit split = (FileSplit) context.getInputSplit();

        // write initial log entry for runtime measurements
        LOG.info(context.getTaskAttemptID() + " starts processing of split " + split);
        final long startTime = System.nanoTime();


        String inputFormat = jobConfig.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        String outputCompression = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        String outputFormat = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
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

        Product product = productFactory.readProduct(split.getPath(), inputFormat);
        String regex = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_REGEX, null);
        String replacement = jobConfig.get(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT, null);
        String newProductName = FileUtils.getFilenameWithoutExtension(split.getPath().getName());
        if (regex != null && replacement != null) {
            String productName = product.getName();
            newProductName = productName.replaceAll(regex, replacement);
        }
        LOG.info("New product name: " + newProductName);
        product.setName(newProductName);

        File tmpDir = new File(System.getProperty("java.io.tmpdir"), "tmpProductDir");
        try {
            if (!tmpDir.mkdir()) {
                throw new IOException("Failed to create tmp directory: " + tmpDir.getAbsolutePath());
            }
            String targetProductName = product.getName();
            File productFile = new File(tmpDir, targetProductName + outputExtension);
            LOG.info("Start writing product to file: " + productFile.getName());
            ProductIO.writeProduct(product, productFile, outputFormat, false, new ProgressMonitorAdapter(context));
            LOG.info("Finished writing product.");

            if ("zip".equals(outputCompression)) {
                LOG.info("Creating ZIP archive on HDFS.");
                OutputStream outputStream = createOutputStream(context, targetProductName + ".zip");
                zip(tmpDir, outputStream, context);
            } else if ("gz".equals(outputCompression)) {
                LOG.info("Creating GZ file on HDFS.");
                InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
                OutputStream outputStream = new GZIPOutputStream(createOutputStream(context, targetProductName + ".gz"));
                copyAndClose(inputStream, outputStream, context);
            } else {
                LOG.info("Copying file to HDFS.");
                InputStream inputStream = new BufferedInputStream(new FileInputStream(productFile));
                OutputStream outputStream = createOutputStream(context, targetProductName + outputExtension);
                copyAndClose(inputStream, outputStream, context);
            }
            LOG.info("Finished writing to HDFS.");
        } finally {
            FileUtils.deleteTree(tmpDir);
            if (product != null) {
                product.dispose();
            }
            final long stopTime = System.nanoTime();
            LOG.info(context.getTaskAttemptID() + " stops processing of split " + split + " after " + ((stopTime - startTime) / 1E9) + " sec");
        }
    }

    private void copyAndClose(InputStream inputStream, OutputStream outputStream, Progressable progressable) throws IOException {
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

    static OutputStream createOutputStream(TaskInputOutputContext<?,?,?,?> context, String filename) throws IOException, InterruptedException {
        Path zipPath = new Path(FileOutputFormat.getWorkOutputPath(context), filename);
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        return fileSystem.create(zipPath, (short) 1);
    }

    static String getNewProductname(String productName, String regex, String replacement) {
        return productName.replaceAll(regex, replacement);
    }

    private static class ProgressMonitorAdapter extends ProgressMonitorWrapper {

        private final Progressable progressable;

        protected ProgressMonitorAdapter(Progressable progressable) {
            super(ProgressMonitor.NULL);
            this.progressable = progressable;
        }

        @Override
        public void worked(int work) {
            progressable.progress();
        }
    }
}
