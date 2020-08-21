/*
 * Copyright (C) 2016 Brockmann Consult GmbH (info@brockmann-consult.de)
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
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.DecodeQualification;
import org.esa.snap.core.dataio.IllegalFileFormatException;
import org.esa.snap.core.dataio.ProductIOPlugInManager;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.awt.Dimension;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A reader for handling Sentinel-2 data on calvalus.
 * Support L1C and L2A (from Sen2Cor)
 * It unzips the products and opens from the local file.
 */
public class Sentinel2CalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private static final String FORMAT_10M = "CALVALUS-SENTINEL-2-MSI-10M";
    public static final String FORMAT_20M = "CALVALUS-SENTINEL-2-MSI-20M";
    private static final String FORMAT_60M = "CALVALUS-SENTINEL-2-MSI-60M";
    private static final String FORMAT_MULTI = "CALVALUS-SENTINEL-2-MSI-MultiRes";

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            if (pathConfig.getPath().toString().startsWith("file://")
                    && ! filename.endsWith(".zip")) {
                return DecodeQualification.UNABLE;
            }
            if ((filename.matches("^S2.*_MSIL1C.*") ||
                    filename.matches("^S2.*_...L2A.*"))
                    && ! filename.endsWith(".nc") && ! filename.endsWith(".tif") && ! filename.endsWith(".dim")) {
                return DecodeQualification.INTENDED;
            }
        }
        return DecodeQualification.UNABLE;
    }

    @Override
    public Class[] getInputTypes() {
        return new Class[]{PathConfiguration.class};
    }

    @Override
    public ProductReader createReaderInstance() {
        return new Sentinel2CalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{FORMAT_10M, FORMAT_20M, FORMAT_60M, FORMAT_MULTI};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".zip"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Sentinel-2 L1C & L2A on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class Sentinel2CalvalusReader extends AbstractProductReader {

        //S2A_OPER_PRD_MSIL1C_PDMC_20161201T211507_R108_V20161201T103412_20161201T103412
        private static final Pattern NAME_TIME_PATTERN = Pattern.compile(".*_V([0-9]{8}T[0-9]{6})_([0-9]{8}T[0-9]{6}).*");
        //S2A_MSIL1C_20161212T100412_N0204_R122_T33UVT_20161212T100409
        private static final Pattern NAME_TIME_PATTERN_L1C = Pattern.compile("S2._MSIL1C_(([0-9]{8}T[0-9]{6})).*");
        private static final Pattern NAME_TIME_PATTERN_L2A = Pattern.compile("S2._...L2A_(([0-9]{8}T[0-9]{6})).*");
        private static final String DATE_FORMAT_PATTERN = "yyyyMMdd'T'HHmmss";
        public static final String FORMAT_L2_SEN2AGRI = "S2_AGRI_SSC_L2VALD";

        Sentinel2CalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration configuration = pathConfig.getConfiguration();
                File localFile = null;
                String snapFormatName = "SENTINEL-2-MSI-MultiRes";
                if ("file".equals(pathConfig.getPath().toUri().getScheme()) && new File(pathConfig.getPath().toUri()).getName().matches("(?:^MTD|.*MTD_SAF).*xml$")) {
                    localFile = new File(pathConfig.getPath().toUri());
                    snapFormatName = "SENTINEL-2-MSI-MultiRes";
                } else if ("file".equals(pathConfig.getPath().toUri().getScheme()) && new File(pathConfig.getPath().toUri()).getName().matches("S2._OPER_SSC_L2VALD_[0-9]{2}[A-Z]{3}____[0-9]{8}.(?:HDR|hdr)$")) {
                    localFile = new File(pathConfig.getPath().toUri());
                    snapFormatName = FORMAT_L2_SEN2AGRI;
                } else if (("s3a".equals(pathConfig.getPath().toUri().getScheme())
                        || "swift".equals(pathConfig.getPath().toUri().getScheme()))
                        && ! pathConfig.getPath().getName().endsWith(".zip")
                        && ! pathConfig.getPath().getName().endsWith(".tar.gz")
                        && ! pathConfig.getPath().getName().endsWith(".tgz")) {
                    // download the folder
                    FileSystem fs = pathConfig.getPath().getFileSystem(configuration);
                    File dst = new File(pathConfig.getPath().getName());
                    LOG.info("copyFileToLocal: " + pathConfig.getPath().toString() + " --> " + dst);
                    long t0 = System.currentTimeMillis();
                    FileUtil.copy(fs, pathConfig.getPath(), dst, false, configuration);
                    LOG.info("time for s3/swift input retrieval [ms]: " + (System.currentTimeMillis() - t0));
                    // TODO: restricted to S2 MSIL1C; support L2A as well, maybe support other Sentinel products and other products
                    // should be done by a ProductIO
                    localFile = new File(dst, "MTD_MSIL1C.xml");
                    snapFormatName = "SENTINEL-2-MSI-MultiRes";
                } else {
                    long t0 = System.currentTimeMillis();
                    File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), configuration);
                    LOG.info("time for zip input retrieval [ms]: " + (System.currentTimeMillis() - t0));

                    // find *MTD*xml file in top directory
                    for (File file : unzippedFiles) {
                        if (file.getName().matches("(?:^MTD|.*MTD_SAF).*xml$")
                                && file.getParentFile() != null
                                && file.getParentFile().getName().endsWith(".SAFE")) {
                            localFile = file;
                            snapFormatName = "SENTINEL-2-MSI-MultiRes";
                            break;
                        } else if (file.getName().endsWith(".dim")) {
                            localFile = file;
                            snapFormatName = "BEAM-DIMAP";
                            break;
                        }
                    }
                    if (localFile == null && pathConfig.getPath().getName().contains("L2A")) {
                        // find *MTD*xml file in top directory
                        for (File file : unzippedFiles) {
                            //S2A_OPER_SSC_L2VALD_14PRC____20180208.HDR
                            if (file.getName().matches("S2._OPER_SSC_L2VALD_[0-9]{2}[A-Z]{3}____[0-9]{8}.(?:HDR|hdr)$")
                                    && file.getParentFile() != null
                                    && file.getParentFile().getName().endsWith(".SAFE")) {
                                localFile = file;
                                snapFormatName = FORMAT_L2_SEN2AGRI;
                                break;
                            }
                        }
                    }
                    if (localFile == null) {
                        throw new IllegalFileFormatException("input has no MTD_SAF file and no S2VALD hdr.");
                    }
                    CalvalusLogger.getLogger().info("productXML file = " + localFile);
                }
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, FORMAT_20M);
                CalvalusLogger.getLogger().info("inputFormat = " + inputFormat);
                Product product;
                product = readProduct(localFile, snapFormatName);

                CalvalusLogger.getLogger().info("Band names: " + Arrays.toString(product.getBandNames()));
                if (product.getStartTime() == null && product.getEndTime() == null) {
                    setTimeFromFilename(product, localFile.getName());
                }

//                // hack so that L3 of Sen2Agri runs. Todo: ensure resampling works with Sen2Agri data!
                if (!inputFormat.equals(FORMAT_MULTI) || (snapFormatName.equals("BEAM-DIMAP") && product.getBand("B2_ac") != null)
//                        && !snapFormatName.equals(FORMAT_L2_SEN2AGRI)
                        ) {
                    // Do not set the product reader, SNAP will else try to read data with it, leads to NPE in JAI
                    //product.setProductReader(this);
                    Map<String, Object> params = new HashMap<>();
                    String referenceBand = null;
                    if ("BEAM-DIMAP".equals(snapFormatName)) {  // TODO: only applicable for multi-resolution S2AC
                        referenceBand = "B2_ac";
                    } else if (!snapFormatName.equals(FORMAT_L2_SEN2AGRI) && inputFormat.equals(FORMAT_10M)) {
                        referenceBand = "B2";
                    } else if (!snapFormatName.equals(FORMAT_L2_SEN2AGRI) && inputFormat.equals(FORMAT_20M)) {
                        referenceBand = "B5";
                    } else if (!snapFormatName.equals(FORMAT_L2_SEN2AGRI) && inputFormat.equals(FORMAT_60M)) {
                        referenceBand = "B1";
                    } else if (snapFormatName.equals(FORMAT_L2_SEN2AGRI) && inputFormat.equals(FORMAT_10M)) {
                        referenceBand = "FRE_R1_B2";
                    } else if (snapFormatName.equals(FORMAT_L2_SEN2AGRI) && inputFormat.equals(FORMAT_20M)) {
                        referenceBand = "FRE_R2_B5";
                    }
                    if (referenceBand == null || !product.containsBand(referenceBand)) {
                        String msg = String.format("Resampling not possible. inputformat=%s productType=%s", inputFormat, product.getProductType());
                        throw new IllegalArgumentException(msg);
                    }
                    params.put("referenceBand", referenceBand);
                    File productFileLocation = product.getFileLocation();
                    Dimension preferredTileSize = product.getPreferredTileSize();
                    CalvalusLogger.getLogger().info("resampling input to " + referenceBand);
                    product = GPF.createProduct("Resample", params, product);
                    //product.setFileLocation(productFileLocation);
                    product.setPreferredTileSize(preferredTileSize);
                }
                return product;
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        static void setTimeFromFilename(Product product, String filename) {
            Pattern pattern;
            String productType = filename.substring(3, 11);
            if ("_MSIL1C_".equals(productType)) {
                pattern = NAME_TIME_PATTERN_L1C;
            } else if ("_MSIL2A_".equals(productType) || "_REVL2A_".equals(productType)) {
                pattern = NAME_TIME_PATTERN_L2A;
            } else {
                pattern = NAME_TIME_PATTERN;
            }
            Matcher matcher = pattern.matcher(filename);
            if (matcher.matches()) {
                try {
                    ProductData.UTC start = ProductData.UTC.parse(matcher.group(1), DATE_FORMAT_PATTERN);
                    ProductData.UTC end = ProductData.UTC.parse(matcher.group(2), DATE_FORMAT_PATTERN);
                    product.setStartTime(start);
                    product.setEndTime(end);
                    CalvalusLogger.getLogger().info("updating time: start = " + start.format() + "  end = " + end.format());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        }


        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }

    static Product readProduct(File xmlFile, String formatPrefix) throws IOException {
        ProductReaderPlugIn productReaderPlugin = findWithFormatPrefix(xmlFile, formatPrefix);
        return productReaderPlugin.createReaderInstance().readProductNodes(xmlFile, null);
    }

    static ProductReaderPlugIn findWithFormatPrefix(File xmlFile, String formatPrefix) throws IOException {
        ProductIOPlugInManager registry = ProductIOPlugInManager.getInstance();
        Iterator<ProductReaderPlugIn> allReaderPlugIns = registry.getAllReaderPlugIns();
        while (allReaderPlugIns.hasNext()) {
            ProductReaderPlugIn readerPlugIn = allReaderPlugIns.next();
            String[] formatNames = readerPlugIn.getFormatNames();
            for (String formatName : formatNames) {
                if (formatName.startsWith(formatPrefix)) {
                    DecodeQualification decodeQualification = readerPlugIn.getDecodeQualification(xmlFile.getAbsoluteFile());
                    if (decodeQualification == DecodeQualification.INTENDED) {
                        CalvalusLogger.getLogger().info("formatName = " + formatName);
                        return readerPlugIn;
                    }
                }
            }
        }
        String msg = String.format("No reader found with format prefix: '%s'", formatPrefix);
        throw new IllegalFileFormatException(msg);
    }
}

