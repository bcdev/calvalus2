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

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A reader for handling Sentinel-2 data on calvalus.
 * Support L1C and L2A (from Sen2Cor)
 * It unzips the products and opens from the local file.
 */
public class Sentinel2CalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_10M = "CALVALUS-SENTINEL-2-MSI-10M";
    private static final String FORMAT_20M = "CALVALUS-SENTINEL-2-MSI-20M";
    private static final String FORMAT_60M = "CALVALUS-SENTINEL-2-MSI-60M";
    private static final String FORMAT_MULTI = "CALVALUS-SENTINEL-2-MSI-MultiRes";

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            if (filename.matches("^S2.*_MSIL1C.*") ||
                    filename.matches("^S2A.*_MSIL2A.*")) {
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
        private static final Pattern NAME_TIME_PATTERN_L2A = Pattern.compile("S2._MSIL2A_(([0-9]{8}T[0-9]{6})).*");
        private static final String DATE_FORMAT_PATTERN = "yyyyMMdd'T'HHmmss";

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
                if ("file".equals(pathConfig.getPath().toUri().getScheme())) {
                    localFile = new File(pathConfig.getPath().toUri());
                } else {
                    File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), configuration);

                    // find *MTD*xml file in top directory
                    for (File file : unzippedFiles) {
                        if (file.getName().matches("(?:^MTD|.*MTD_SAF).*xml$")) {
                            File parentFile = file.getParentFile();
                            if (parentFile != null && parentFile.getName().endsWith(".SAFE")) {
                                localFile = file;
                                break;
                            }
                        }
                    }
                    if (localFile == null) {
                        throw new IllegalFileFormatException("input has no MTD_SAF file.");
                    }
                    CalvalusLogger.getLogger().info("productXML file = " + localFile);
                }
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, FORMAT_20M);
                CalvalusLogger.getLogger().info("inputFormat = " + inputFormat);
                Product product;
                product = readProduct(localFile, "SENTINEL-2-MSI-MultiRes");
                CalvalusLogger.getLogger().info("Band names: " + Arrays.toString(product.getBandNames()));
                File productFileLocation = product.getFileLocation();
                if (product.getStartTime() == null && product.getEndTime() == null) {
                    setTimeFromFilename(product, localFile.getName());
                }
                if (!inputFormat.equals(FORMAT_MULTI)) {
                    product.setProductReader(this);
                    Map<String, Object> params = new HashMap<>();
                    if (inputFormat.equals(FORMAT_10M) && product.containsBand("B2")) {
                        params.put("referenceBand", "B2");
                    } else if (inputFormat.equals(FORMAT_20M) && product.containsBand("B5")) {
                        params.put("referenceBand", "B5");
                    } else if (inputFormat.equals(FORMAT_60M) && product.containsBand("B1")) {
                        params.put("referenceBand", "B1");
                    } else {
                        String msg = String.format("Resampling not possible. inputformat=%s productType=%s", inputFormat, product.getProductType());
                        throw new IllegalArgumentException(msg);
                    }
                    product = GPF.createProduct("Resample", params, product);
                    product.setFileLocation(productFileLocation);
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
            } else if ("_MSIL2A_".equals(productType)) {
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
                    DecodeQualification decodeQualification = readerPlugIn.getDecodeQualification(xmlFile);
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

