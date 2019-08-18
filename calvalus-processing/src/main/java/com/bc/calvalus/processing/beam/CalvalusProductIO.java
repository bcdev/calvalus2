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
import com.bc.ceres.glevel.MultiLevelImage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.Mask;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.datamodel.VirtualBand;
import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarInputStream;
import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.LogRecord;
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
        long t1 = System.currentTimeMillis();
        LOG.info(String.format("readProduct: %s [%s]", pathConf.getPath(), inputFormat));
        Product product = readProductImpl(pathConf, PathConfiguration.class, inputFormat);
        if (product == null) {
            final Path path = pathConf.getPath();
            final Configuration configuration = pathConf.getConfiguration();
            ImageInputStream imageInputStream = openImageInputStream(path, configuration);
            product = readProductImpl(imageInputStream, ImageInputStream.class, inputFormat);
            if (product == null) {
                File localFile;
                if ("file".equals(path.toUri().getScheme())) {    // TODO: bad criterion for whether file is "local"
                    localFile = new File(path.toUri());
                } else if (("s3a".equals(path.toUri().getScheme())
                            || "swift".equals(path.toUri().getScheme())) && "MTD_MSIL1C.xml".equals(path.getName())) {
                    localFile = copyFileToLocal(path.getParent(), configuration);
                } else {
                    localFile = copyFileToLocal(path, configuration);
                }
                maybeUpdateTilingProperties(localFile);  // TODO: maybe add to other locations where product file is opened
                product = readProductImpl(localFile, File.class, inputFormat);
            }
        }
        if (product == null) {
            Iterator<ProductReaderPlugIn> it = ProductIOPlugInManager.getInstance().getAllReaderPlugIns();
            while (it.hasNext()) {
                ProductReaderPlugIn readerPlugIn = it.next();
                LOG.info("reader candidate " + readerPlugIn + ' ' + Arrays.toString(readerPlugIn.getFormatNames()) + ' ' + Arrays.toString(readerPlugIn.getInputTypes()));
            }
            throw new IOException(String.format("No reader found for product: '%s'", pathConf.getPath().toString()));
        }
        final Path path = pathConf.getPath();
        String pathName = path.getName();
        if (pathName.startsWith("CCI-Fire-MERIS-SDR-L3") && pathName.endsWith(".nc")) {
            LOG.info("readProduct: Product " + pathName + " has no time information...extracting it from file name...");
            setDateToMerisSdrProduct(product, pathName);
            LOG.info(String.format("readProduct: ...done. Product start time: %s; product end time: %s", product.getStartTime().format(), product.getEndTime().format()));
        }
        LOG.info(String.format("readProduct: Opened product width = %d height = %d", product.getSceneRasterWidth(), product.getSceneRasterHeight()));
        if (product.getSceneGeoCoding() == null) {
            LOG.warning("readProduct: GeoCoding: NULL");
        }
        long t2 = System.currentTimeMillis();
        LOG.info(String.format("readProduct: took %,d ms for %s", t2 - t1, path));
        printProductOnStdout(product, "opened from " + pathConf.getPath());
        return product;
    }

    private static void maybeUpdateTilingProperties(File localFile) {
        if (localFile.getName().endsWith(".nc")
                && "32".equals(System.getProperty("snap.dataio.reader.tileWidth"))
                && "32".equals(System.getProperty("snap.dataio.reader.tileHeight"))) {
            NetcdfFile netcdfFile = null;
            try {
                netcdfFile = NetcdfFile.open(localFile.getPath());
                Attribute tileSizeAttribute = netcdfFile.findGlobalAttribute("TileSize");
                if (tileSizeAttribute != null) {
                    String[] tileSizeValues = tileSizeAttribute.getStringValue().split(":");
                    System.setProperty("snap.dataio.reader.tileWidth", tileSizeValues[0]);
                    System.setProperty("snap.dataio.reader.tileHeight", tileSizeValues[1]);
                    LOG.info("tile size adjusted from 32:32 to " + tileSizeAttribute.getStringValue());
                } else {
                    LOG.info("tile size not adjusted from 32:32, product has no attribute TileSize");
                }
            } catch (Exception e) {
                LOG.warning("failed to adjusted tile size from 32:32 to product chunking");
            } finally {
                if (netcdfFile != null) {
                    try {
                        netcdfFile.close();
                    } catch (IOException e) {
                    }
                }
            }
        }
    }

    // currently not used
    private static void logAllAvailableProductReaders() {
        LOG.severe("No reader found. Available plugin classes:");
        List<String> readerNames = new ArrayList<>();
        Iterator<ProductReaderPlugIn> allReaderPlugIns = ProductIOPlugInManager.getInstance().getAllReaderPlugIns();
        while (allReaderPlugIns.hasNext()) {
            readerNames.add(allReaderPlugIns.next().getClass().getName());
        }
        readerNames.sort(String::compareTo);
        for (String readerName : readerNames) {
            LOG.severe("    " + readerName);
        }
    }

    /**
     * Copies the file given to the local input directory for access as a ordinary {@code File}.
     *
     * @param path The path to the file in the HDFS.
     * @param conf The Hadoop configuration.
     */
    public static File copyFileToLocal(Path path, Configuration conf) throws IOException {
        File localFile = new File(".", path.getName());
        return copyFileToLocal(path, localFile, conf);
    }

    public static File copyFileToLocal(Path path, File localFile, Configuration conf) throws IOException {
        LOG.info("copyFileToLocal: " + path + " --> " + localFile);
        if (localFile.exists()) {
            LOG.info("copyFileToLocal: File already exist");
        } else {
            if ("file".equals(path.toUri().getScheme())) {
                LOG.info("copyFileToLocal: creating symlink");
                FileUtil.symLink(path.toString(), localFile.getAbsolutePath());                
            } else {
                FileSystem fs = path.getFileSystem(conf);
                FileUtil.copy(fs, path, localFile, false, conf);
            }
        }
        return localFile;
    }

    public static File[] uncompressArchiveToCWD(Path path, Configuration conf) throws IOException {
        return uncompressArchiveToDir(path, new File("."), conf);
    }

    public static File[] uncompressArchiveToDir(Path path, File localDir, Configuration conf) throws IOException {
        long t1 = System.currentTimeMillis();
        FileSystem fs = path.getFileSystem(conf);
        InputStream inputStream = new BufferedInputStream(fs.open(path));
        List<File> extractedFiles = new ArrayList<>();

        String archiveName = path.getName().toLowerCase();
        long localSize = 0;
        boolean isZippedSlstrWithoutExtension = path.getName().matches("S3._SL_1_RBT.*_NT_00.");
        if (archiveName.endsWith(".zip") || isZippedSlstrWithoutExtension) {
            try (ZipInputStream zipIn = new ZipInputStream(inputStream)) {
                ZipEntry entry;
                while ((entry = zipIn.getNextEntry()) != null) {
                    extractedFiles.add(handleEntry(localDir, entry.getName(), entry.isDirectory(), zipIn));
                    localSize += entry.getSize();
                }
            }
        } else if (isTarCompressed(archiveName)) {
            try (TarInputStream tarIn = getTarInputStream(archiveName, inputStream)) {
                TarEntry entry;
                while ((entry = tarIn.getNextEntry()) != null) {
                    extractedFiles.add(handleEntry(localDir, entry.getName(), entry.isDirectory(), tarIn));
                    localSize += entry.getSize();
                    
                }
            }
        } else {
            throw new IOException("unsupported archive format: " + archiveName);
        }
        long t2 = System.currentTimeMillis();
        LOG.info(String.format("uncompressArchiveToDir: size %,d bytes  took %,d ms  from %s", localSize, t2 - t1, path));
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

    private static boolean isTbz(String name) {
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
        return new FSImageInputStream(in, status.getLen(), path.toString());
    }

    private static Product readProductImpl(Object input, Class<?> inputClass, String inputFormat) {
        Iterator<ProductReaderPlugIn> it = ProductIOPlugInManager.getInstance().getAllReaderPlugIns();
        ProductReaderPlugIn selectedPlugIn = null;
        while (it.hasNext()) {
            ProductReaderPlugIn readerPlugIn = it.next();
            if (canHandle(readerPlugIn, inputClass)) {
                if (inputFormat != null && !supportsFormat(readerPlugIn, inputFormat)) {
                    continue;
                }
                try {
                    DecodeQualification decodeQualification = readerPlugIn.getDecodeQualification(input);
                    if (decodeQualification == DecodeQualification.INTENDED) {
                        selectedPlugIn = readerPlugIn;
                        break;
                    } else if (decodeQualification == DecodeQualification.SUITABLE) {
                        selectedPlugIn = readerPlugIn;
                    }
                } catch (Exception e) {
                    LOG.severe("readProductImpl: Error attempting to read " + input + " with plugin reader " + readerPlugIn.toString() + ": " + e.getMessage());
                }
            }
        }
        if (selectedPlugIn != null) {
            ProductReader productReader = selectedPlugIn.createReaderInstance();
            try {
                return productReader.readProductNodes(input, null);
            } catch (IOException e) {
                String msg = String.format("Exception from productReader.readProductNodes from %s", input);
                LogRecord lr = new LogRecord(Level.WARNING, msg);
                lr.setSourceClassName("com.bc.calvalus.processing.beam.CalvalusProductIO");
                lr.setSourceMethodName("readProductImpl");
                lr.setThrown(e);
                LOG.log(lr);
                return null;
            }
        } else {
            LOG.info("looked up reader plugin for " + inputClass + " " + inputFormat);
            return null;
        }
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

    private static boolean supportsFormat(ProductReaderPlugIn readerPlugIn, String inputFormat) {
        for (String otherFormatName : readerPlugIn.getFormatNames()) {
            if (otherFormatName.equalsIgnoreCase(inputFormat)) {
                return true;
            }
        }
        return false;
    }

    public static void printProductOnStdout(Product p, String origin) {
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println(origin);
        if (p == null) {
            System.out.println("product is NULL");
            System.out.println("-----------------------------------------------------------------------------------------");
            return;
        }
        Dimension preferredTileSize = p.getPreferredTileSize();
        String productTileSize = "";
        if (preferredTileSize != null) {
            productTileSize = String.format("[%d,%d]", preferredTileSize.width, preferredTileSize.height);
        }
        System.out.printf("Product: %s (%d,%d)%s%n",
                p.getName(),
                p.getSceneRasterWidth(),
                p.getSceneRasterHeight(),
                productTileSize
        );
        ProductReader productReader = p.getProductReader();
        if (productReader != null) {
            System.out.printf("ProductReader: %s%n", productReader.toString());
            ProductReaderPlugIn readerPlugIn = productReader.getReaderPlugIn();
            if (readerPlugIn != null) {
                System.out.printf("ProductReaderPlugin: %s%n", readerPlugIn.toString());
            }
        }
        File fileLocation = p.getFileLocation();
        if (fileLocation != null) {
            System.out.printf("FileLocation: %s%n", fileLocation);
        }
        GeoCoding geoCoding = p.getSceneGeoCoding();
        if (geoCoding != null) {
            System.out.printf("GeoCoding: %s%n", geoCoding.getClass().getSimpleName());
        } else {
            System.out.println("GeoCoding: NULL");
        }
        System.out.println("RasterDataNodes:");
        int i = 0;
        List<RasterDataNode> rasterDataNodes = p.getRasterDataNodes();
        for (RasterDataNode rdn : rasterDataNodes) {
            if (++i > 3) {
                System.out.println("...");
                break;
            }
            String rdnTileSize = "";
            if (rdn.isSourceImageSet()) {
                MultiLevelImage image = rdn.getSourceImage();
                rdnTileSize = String.format("[%d,%d]", image.getTileWidth(), image.getTileHeight());
            }
            String[] rasterDataNodeInfo = getRasterDataNodeInfo(rdn);
            String rdnType = rasterDataNodeInfo[0];
            String rdnDesc = "";
            if (rasterDataNodeInfo.length == 2) {
                rdnDesc = " {" + rasterDataNodeInfo[1] + "}";
            }
            System.out.printf("    %s %s:%s (%d,%d)%s%s%n",
                              rdnType,
                              rdn.getName(),
                              ProductData.getTypeString(rdn.getDataType()),
                              rdn.getRasterWidth(),
                              rdn.getRasterHeight(),
                              rdnTileSize,
                              rdnDesc
            );
        }
        System.out.println("-----------------------------------------------------------------------------------------");
    }
    
    private static String[] getRasterDataNodeInfo(RasterDataNode rdn) {
        if (rdn instanceof VirtualBand) {
            return new String[]{"V", ((VirtualBand) rdn).getExpression()};
        } else if (rdn instanceof Mask) {
            Mask mask = (Mask) rdn;
            Mask.ImageType imageType = mask.getImageType();
            String desc = imageType.getName();
            if (imageType instanceof Mask.RangeType) {
                desc += ": " + Mask.RangeType.getExpression(mask);
            } else if (imageType instanceof Mask.BandMathsType) {
                desc += ": " + Mask.BandMathsType.getExpression(mask);                
            } else if (imageType instanceof Mask.VectorDataType) {
                desc += ": vectorData=" + Mask.VectorDataType.getVectorData(mask).getName();                
            }
            return new String[]{"M", desc};
        } else if (rdn instanceof TiePointGrid) {
            return new String[]{"T"};
        } else if (rdn instanceof Band) {
            Band band = (Band) rdn;
            if (band.isFlagBand()) {
                String flags = String.join("|", band.getFlagCoding().getFlagNames());
                return new String[]{"F", flags};
            } else if (band.isIndexBand()) {
                String indices = String.join("|", band.getIndexCoding().getIndexNames());
                return new String[]{"I", indices};
            }
            return new String[]{"B"};
        } else {
            return new String[]{"R"};
        }
    }
}
