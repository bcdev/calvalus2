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

package com.bc.calvalus.processing.analysis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProgressSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.snap.framework.datamodel.Product;
import org.esa.snap.util.io.FileUtils;

import javax.imageio.ImageIO;
import java.awt.image.RenderedImage;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A mapper for generating quick-looks of products.
 */
public class QLMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final String FILE_SYSTEM_COUNTERS = "FileSystemCounters";
    private static final String FILE_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";

    public static final Logger LOGGER = CalvalusLogger.getLogger();

    static {
        try {
            // Make "hdfs:" a recognised URL protocol
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable e) {
            // ignore as it is most likely already set
            String msg = String.format("Cannot set URLStreamHandlerFactory (message: '%s'). " +
                                       "This may not be a problem because it is most likely already set.",
                                       e.getMessage());
            LOGGER.fine(msg);
        }
    }

    @Override
    public void run(Mapper.Context context) throws IOException, InterruptedException {
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProgressSplitProgressMonitor(context);
        pm.beginTask("Image generation", 100);
        try {
            Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 5));
            if (product != null) {
                String productName = FileUtils.getFilenameWithoutExtension(processorAdapter.getInputPath().getName());
                Quicklooks.QLConfig[] configs = Quicklooks.get(context.getConfiguration());
                for (Quicklooks.QLConfig config : configs) {
                    String imageFileName = productName + "_" + config.getBandName();
                    createQuicklook(product, imageFileName, context, config);
                }
            }
        } finally {
            pm.done();
            processorAdapter.dispose();
        }
    }

    public static void createQuicklook(Product product, String imageFileName, Mapper.Context context,
                                       Quicklooks.QLConfig config) {
        try {
            RenderedImage quicklookImage = QuicklookGenerator.createImage(context, product, config);
            if (quicklookImage != null) {
                OutputStream outputStream = createOutputStream(context, imageFileName + "." + config.getImageType());
                OutputStream pmOutputStream = new BytesCountingOutputStream(outputStream, context);
                try {
                    ImageIO.write(quicklookImage, config.getImageType(), pmOutputStream);
                } finally {
                    outputStream.close();
                }
            }
        } catch (Exception e) {
            String msg = String.format("Could not create quicklook image '%s'.", config.getBandName());
            LOGGER.log(Level.WARNING, msg, e);
        }
    }

    private static OutputStream createOutputStream(Mapper.Context context, String fileName) throws Exception {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        final FSDataOutputStream fsDataOutputStream = path.getFileSystem(context.getConfiguration()).create(path);
        return new BufferedOutputStream(fsDataOutputStream);
    }


    private static class BytesCountingOutputStream extends OutputStream {

        private final OutputStream wrappedStream;
        private final Mapper.Context context;
        private int countedBytes;

        public BytesCountingOutputStream(OutputStream outputStream, Mapper.Context context) {
            wrappedStream = outputStream;
            this.context = context;
        }

        @Override
        public void write(int b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(1);
        }

        @Override
        public void write(byte[] b) throws IOException {
            wrappedStream.write(b);
            maybeIncrementHadoopCounter(b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            wrappedStream.write(b, off, len);
            maybeIncrementHadoopCounter(len - off);
        }

        @Override
        public void close() throws IOException {
            wrappedStream.close();
            incrementHadoopCounter();
        }

        @Override
        public void flush() throws IOException {
            wrappedStream.flush();
            incrementHadoopCounter();
        }


        private void maybeIncrementHadoopCounter(int byteCount) {
            countedBytes += byteCount;
            if (countedBytes / (1024 * 10) >= 1) {
                incrementHadoopCounter();
            }
        }

        private void incrementHadoopCounter() {
            context.getCounter(FILE_SYSTEM_COUNTERS, FILE_BYTES_WRITTEN).increment(countedBytes);
            context.progress();
            countedBytes = 0;
        }
    }


}
