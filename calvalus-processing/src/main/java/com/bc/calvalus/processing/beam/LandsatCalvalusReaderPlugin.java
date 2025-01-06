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
import org.esa.snap.core.dataio.ProductIO;
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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * A reader for handing Landsat data on calvalus.
 * It unzips the products and open from the local file.
 */
public class LandsatCalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_15M = "CALVALUS-Landsat-15M";
    public static final String FORMAT_30M = "CALVALUS-Landsat-30M";
    private static final String FORMAT_MULTI = "CALVALUS-Landsat";

    private static final String COLLECTION_FILENAME_REGEX = "L[COTEM]\\d{2}_L1\\w{2}_\\d{3}\\d{3}_\\d{8}_\\d{8}_\\d{2}_(T1|T2|RT)";
    private static final String L4_FILENAME_REGEX = "LT4\\d{13}\\w{3}\\d{2}";
    private static final String L5_FILENAME_REGEX = "LT5\\d{13}\\w{3}\\d{2}";
    private static final String L7_FILENAME_REGEX = "LE7\\d{13}\\w{3}\\d{2}";
    private static final String L8_FILENAME_REGEX = "L[OTC]8\\d{13}\\w{3}\\d{2}";
    private static final String COMPR_FILENAME_REGEX = "\\.(tar\\.gz|tgz|tar\\.bz|tbz|tar\\.bz2|tbz2|zip|ZIP)";

    public static final String[] FILENAME_PATTERNS = {
            L4_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L5_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L7_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L8_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            COLLECTION_FILENAME_REGEX + COMPR_FILENAME_REGEX
    };

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            for (String pattern : FILENAME_PATTERNS) {
                if (filename.matches(pattern)) {
                    return DecodeQualification.INTENDED;
                }
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
        return new LandsatCalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{FORMAT_15M, FORMAT_30M, FORMAT_MULTI};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".none"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Landsat on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class LandsatCalvalusReader extends AbstractProductReader {

        LandsatCalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
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
                    // find manifest file
                    for (File file : unzippedFiles) {
                        if (file.getName().toLowerCase().endsWith("_mtl.txt")) {
                            localFile = file;
                            break;
                        }
                    }
                    if (localFile == null) {
                        throw new IllegalFileFormatException("input has no MTL file.");
                    }
                }
                final char platform = pathConfig.getPath().getName().charAt(2);
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, FORMAT_30M);
                System.out.println("inputFormat = " + inputFormat + " platform = " + platform);

                String referenceBand;
                switch (inputFormat) {
                    case FORMAT_15M:
                        referenceBand = (platform == '7') ? "radiance_7" : "panchromatic";
                        CalvalusLogger.getLogger().info("reading with format Landsat8GeoTIFF15m and reference band " + referenceBand);
                        return resample(ProductIO.readProduct(localFile, "Landsat8GeoTIFF15m"), referenceBand);
                    case FORMAT_30M:
                        referenceBand = (platform == '7') ? "radiance_3" : "red";
                        CalvalusLogger.getLogger().info("reading with format Landsat8GeoTIFF30m and reference band " + referenceBand);
                        return resample(ProductIO.readProduct(localFile, "Landsat8GeoTIFF30m"), referenceBand);
                    default:
                        CalvalusLogger.getLogger().info("reading with automatic format (inputFormat=" + inputFormat + ")");
                        return ProductIO.readProduct(localFile);
                }
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        private Product resample(Product product, String referenceBand) {
            Map<String, Object> params = new HashMap<>();
            params.put("referenceBand", referenceBand);
            Dimension preferredTileSize = product.getPreferredTileSize();
            CalvalusLogger.getLogger().info("resampling input to " + referenceBand);
            product = GPF.createProduct("Resample", params, product);
            product.setPreferredTileSize(preferredTileSize);
            return product;
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

