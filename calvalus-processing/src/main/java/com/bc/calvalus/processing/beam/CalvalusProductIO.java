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
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.FSImageInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarInputStream;
import ucar.unidata.io.bzip2.CBZip2InputStream;

import javax.imageio.stream.ImageInputStream;
import java.awt.*;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Handles reading of products.
 * Delegates mostly to {@link org.esa.snap.core.dataio.ProductIO}
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
        if (inputFormat != null) {
            LOG.info("Trying to find reader for inputFormat: " + inputFormat);
        }
        Product product = readProductImpl(pathConf, PathConfiguration.class, inputFormat);
        if (product == null) {
            final Path path = pathConf.getPath();
            final Configuration configuration = pathConf.getConfiguration();
            ImageInputStream imageInputStream = openImageInputStream(path, configuration);
            product = readProductImpl(imageInputStream, ImageInputStream.class, inputFormat);
            if (product == null) {
                File localFile;
                if (path.getFileSystem(configuration).getScheme().equals("file")) {
                    localFile = new File(path.toUri());
                } else {
                    localFile = copyFileToLocal(path, configuration);
                }
                product = readProductImpl(localFile, File.class, inputFormat);
            }
        }
        if (product == null) {
            LOG.info("No reader found. Available plugin classes:");
            Iterator<ProductReaderPlugIn> allReaderPlugIns = ProductIOPlugInManager.getInstance().getAllReaderPlugIns();
            while (allReaderPlugIns.hasNext()) {
                ProductReaderPlugIn readerPlugIn = allReaderPlugIns.next();
                String name = readerPlugIn.getClass().getName();
                LOG.info(name);
            }
            throw new IOException(String.format("No reader found for product: '%s'", pathConf.getPath().toString()));
        }
        final Path path = pathConf.getPath();
        String pathName = path.getName();
        if (pathName.startsWith("CCI-Fire-MERIS-SDR-L3") && pathName.endsWith(".nc")) {
            LOG.info("Product " + pathName + " has no time information...extracting it from file name...");
            setDateToMerisSdrProduct(product, pathName);
            LOG.info(String.format("...done. Product start time: %s; product end time: %s", product.getStartTime().format(), product.getEndTime().format()));
        }
        LOG.info(String.format("Opened product width = %d height = %d", product.getSceneRasterWidth(), product.getSceneRasterHeight()));
        Dimension tiling = product.getPreferredTileSize();
        if (tiling != null) {
            LOG.info(String.format("Tiling: width = %d height = %d", (int) tiling.getWidth(), (int) tiling.getHeight()));
        } else {
            LOG.info("Tiling: NONE");
        }
        ProductReader productReader = product.getProductReader();
        if (productReader != null) {
            LOG.info(String.format("ProductReader: %s", productReader.toString()));
            LOG.info(String.format("ProductReaderPlugin: %s", productReader.getReaderPlugIn().toString()));
        }
        GeoCoding geoCoding = product.getSceneGeoCoding();
        if (geoCoding != null) {
            LOG.info(String.format("GeoCoding: %s", geoCoding.toString()));
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
            FileUtil.copy(fs, path, localFile, false, conf);
        } else {
            LOG.info("File already exist");
        }
        return localFile;
    }

    public static File[] uncompressArchiveToCWD(Path path, Configuration conf) throws IOException {
        return uncompressArchiveToDir(path, new File("."), conf);
    }

    public static File[] uncompressArchiveToDir(Path path, File localDir, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        InputStream inputStream = new BufferedInputStream(fs.open(path));
        List<File> extractedFiles = new ArrayList<>();

        String archiveName = path.getName().toLowerCase();
        if (archiveName.endsWith(".zip")) {
            try (ZipInputStream zipIn = new ZipInputStream(inputStream)) {
                ZipEntry entry;
                while ((entry = zipIn.getNextEntry()) != null) {
                    extractedFiles.add(handleEntry(localDir, entry.getName(), entry.isDirectory(), zipIn));
                }
            }
        } else if (isTarCompressed(archiveName)) {
            try (TarInputStream tarIn = getTarInputStream(archiveName, inputStream)) {
                TarEntry entry;
                while ((entry = tarIn.getNextEntry()) != null) {
                    extractedFiles.add(handleEntry(localDir, entry.getName(), entry.isDirectory(), tarIn));
                }
            }
        } else {
            throw new IOException("unsupported archive format: " + archiveName);
        }
        return extractedFiles.toArray(new File[0]);
    }

    private static File handleEntry(File localDir, String name, boolean isDirectory, InputStream zipIn) throws IOException {
        File file = new File(localDir, name);
        if (isDirectory) {
            file.mkdirs();
        } else {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }
            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
                IOUtils.copyBytes(zipIn, out, 8192);
            }
        }
        return file;
    }

    private static TarInputStream getTarInputStream(String archiveName, InputStream inputStream) throws IOException {
        if (isTgz(archiveName)) {
            return new TarInputStream(new GZIPInputStream(inputStream));
        } else if (isTbz(archiveName)) {
            return new TarInputStream(new CBZip2InputStream(inputStream, true));
        } else {
            return new TarInputStream(inputStream);
        }
    }

    private static boolean isTarCompressed(String name) {
        return isTar(name) || isTgz(name) || isTbz(name);
    }

    private static boolean isTar(String name) {
        return name.endsWith(".tar");
    }

    private static boolean isTgz(String name) {
        return name.endsWith(".tar.gz") || name.endsWith(".tgz");
    }

    static boolean isTbz(String name) {
        return name.endsWith(".tar.bz") || name.endsWith(".tbz") || name.endsWith(".tar.bz2") || name.endsWith(".tbz2");
    }


    static void setDateToMerisSdrProduct(Product product, String pathName) throws IOException {
        try {
            int beginIndex = "CCI-Fire-MERIS-SDR-L3-300m-v1.0-".length();
            int endIndex = beginIndex + 10;
            String timeString = pathName.substring(beginIndex, endIndex);
            ProductData.UTC day = ProductData.UTC.parse(timeString, "yyyy-MM-dd");
            product.setStartTime(day);
            product.setEndTime(day);
        } catch (ParseException e) {
            throw new IOException(e);
        }
    }


    private static ImageInputStream openImageInputStream(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(path);
        FSDataInputStream in = fs.open(path);
        return new FSImageInputStream(in, status.getLen());
    }

    private static Product readProductImpl(Object input, Class<?> inputClass, String inputFormat) {
        if (inputFormat != null) {
            return readProductWithInputFormat(input, inputClass, inputFormat);
        } else {
            return readProductWithAutodetect(input);
        }
    }

    private static Product readProductWithInputFormat(Object input, Class<?> inputClass, String inputFormat) {
        ProductReader productReader = ProductIO.getProductReader(inputFormat);
        if (productReader != null) {
            ProductReaderPlugIn readerPlugIn = productReader.getReaderPlugIn();
            if (canHandle(readerPlugIn, inputClass)) {
                try {
                    // TODO Is it ok to set this in the shared configuration?
                    if (input instanceof PathConfiguration) {
                        ((PathConfiguration) input).getConfiguration().set(JobConfigNames.CALVALUS_INPUT_FORMAT, inputFormat);
                    }
                    return productReader.readProductNodes(input, null);
                } catch (IOException e) {
                    return null;
                }
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

    private static Product readProductWithAutodetect(Object input) {
        ProductReader productReader = ProductIO.getProductReaderForInput(input);
        if (productReader != null) {
            try {
                return productReader.readProductNodes(input, null);
            } catch (IOException e) {
                return null;
            }
        }
        return null;
    }
}
