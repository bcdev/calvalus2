/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.datamodel.Product;

import javax.imageio.stream.ImageInputStream;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Handles reading of products.
 * Delegates mostly to {@link org.esa.beam.framework.dataio.ProductIO}
 */
public class CalvalusProductIO {

    private static final Logger LOG = CalvalusLogger.getLogger();

    /**
     * Reads a product from the distributed file system.
     *
     * @param path        The input path
     * @param conf        The Hadoop configuration
     * @param inputFormat The input format, may be {@code null}. If {@code null}, the file format will be detected.
     * @return The product The product read.
     * @throws java.io.IOException If an I/O error occurs
     */
    public static Product readProduct(Path path, Configuration conf, String inputFormat) throws IOException {
        return readProduct(new PathConfiguration(path, conf), inputFormat);
    }

    /**
     * Reads a product from the distributed file system.
     *
     * @param pathConf    Encapsulates a Hadoop Path and a Configuration
     * @param inputFormat The input format, may be {@code null}. If {@code null}, the file format will be detected.
     * @return The product The product read.
     * @throws java.io.IOException If an I/O error occurs
     */
    public static Product readProduct(PathConfiguration pathConf, String inputFormat) throws IOException {
        Product product = readProductImpl(pathConf, PathConfiguration.class, inputFormat);
        if (product == null) {
            ImageInputStream imageInputStream = openImageInputStream(pathConf.getPath(), pathConf.getConfiguration());
            product = readProductImpl(imageInputStream, ImageInputStream.class, inputFormat);
            if (product == null) {
                File localFile = copyFileToLocal(pathConf.getPath(), pathConf.getConfiguration());
                product = readProductImpl(localFile, File.class, inputFormat);
            }
        }
        if (product == null) {
            throw new IOException(String.format("No reader found for product: '%s'", pathConf.getPath().toString()));
        }
        LOG.info(String.format("Opened product width = %d height = %d",
                               product.getSceneRasterWidth(),
                               product.getSceneRasterHeight()));
        ProductReader productReader = product.getProductReader();
        if (productReader != null) {
            LOG.info(String.format("ReaderPlugin: %s", productReader.toString()));
        }
        return product;
    }

    /**
     * Copies the file given to the local input directory for access as a ordinary {@code File}.
     *
     * @param path The path to the file in the HDFS.
     * @param conf The Hadoop configuration.
     * @throws IOException
     */
    public static File copyFileToLocal(Path path, Configuration conf) throws IOException {
        File localFile = new File(".", path.getName());
        return copyFileToLocal(path, localFile, conf);
    }

    public static File copyFileToLocal(Path path, File localFile, Configuration conf) throws IOException {
        LOG.info("Copying file to local: " + path + " --> " + localFile);
        if (!localFile.exists()) {
            FileSystem fs = path.getFileSystem(conf);
            String scheme = path.toUri().getScheme();
            if (scheme != null && scheme.startsWith("file:")) {
                LOG.info("Creating symlink");
                FileUtil.symLink(path.toString(), localFile.getAbsolutePath());
            } else {
                FileUtil.copy(fs, path, localFile, false, conf);
            }
        } else {
            LOG.info("File already exist");
        }
        return localFile;
    }


    private static ImageInputStream openImageInputStream(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(path);
        FSDataInputStream in = fs.open(path);
        return new FSImageInputStream(in, status.getLen());
    }

    private static Product readProductImpl(Object input, Class<?> inputClass, String inputFormat) throws IOException {
        if (inputFormat != null) {
            return readProductWithInputFormat(input, inputClass, inputFormat);
        } else {
            return readProductWithAutodetect(input);
        }
    }

    private static Product readProductWithInputFormat(Object input, Class<?> inputClass, String inputFormat) throws IOException {
        ProductReader productReader = ProductIO.getProductReader(inputFormat);
        if (productReader != null) {
            ProductReaderPlugIn readerPlugIn = productReader.getReaderPlugIn();
            if (canHandle(readerPlugIn, inputClass)) {
                return productReader.readProductNodes(input, null);
            }
        }
        return null;
    }

    private static boolean canHandle(ProductReaderPlugIn readerPlugIn, Class<?> inputClass) {
        if (readerPlugIn != null) {
            Class<?>[] inputTypes = readerPlugIn.getInputTypes();
            for (Class<?> inputType : inputTypes) {
                if (inputType.isAssignableFrom(inputClass)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Product readProductWithAutodetect(Object input) throws IOException {
        ProductReader productReader = ProductIO.getProductReaderForInput(input);
        if (productReader != null) {
            return productReader.readProductNodes(input, null);
        }
        return null;
    }
}
