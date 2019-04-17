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
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.Locale;


/**
 * A reader for handing Sentinel-3 data on calvalus.
 * It unzips the products and open from the local file.
 */
public class Sentinel3CalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_NAME_S3 = "CALVALUS-SENTINEL-3";
    private static final String FORMAT_NAME_SLSTRL1B_500m = "CALVALUS-SENTINEL-3-SLSTRL1B_500m";
    private static final String FORMAT_NAME_SLSTRL1B_1km = "CALVALUS-SENTINEL-3-SLSTRL1B-1km";
    
    private static final String[] FILENAME_PATTERNS = {
            // Sentinel-3 products
            "^S3.?_(OL_1_E[FR]R|OL_2_(L[FR]R|W[FR]R)|SL_1_RBT|SL_2_(LST|WCT|WST)|SY_1_SYN|SY_2_(VGP|SYN)|SY_[23]_VG1)_.*",
            // MERIS Level 1 in Sentinel-3 product format
            "^ENV_ME_1_(F|R)R(G|P).*",
            // MERIS Level 2 in Sentinel-3 product format
            "^ENV_ME_2_(F|R)R(G|P).*",
            // Sentinel-3 SLSTR L1B products in 1km resolution
            "^S3.?_SL_1_RBT_.*",
            // Sentinel-3 SLSTR L1B products in 500 m resolution
            "^S3.?_SL_1_RBT_.*"
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
        return new Sentinel3CalvalusReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{FORMAT_NAME_S3, FORMAT_NAME_SLSTRL1B_500m, FORMAT_NAME_SLSTRL1B_1km};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".none"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "Sentinel-3 on Calvalus";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    static class Sentinel3CalvalusReader extends AbstractProductReader {

        Sentinel3CalvalusReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                Configuration configuration = pathConfig.getConfiguration();
                File[] unzippedFiles;
                if (pathConfig.getPath().getName().endsWith(".zip")) {
                    unzippedFiles = CalvalusProductIO.uncompressArchiveToCWD(pathConfig.getPath(), configuration);
                } else {
                    FileSystem fs = pathConfig.getPath().getFileSystem(configuration);
                    File dst = new File(pathConfig.getPath().getName());
                    CalvalusLogger.getLogger().info("copyFileToLocal: " + pathConfig.getPath().toString() + " --> " + dst);
                    FileUtil.copy(fs, pathConfig.getPath(), dst, false, configuration);
                    unzippedFiles = dst.listFiles();
                }
                // find manifest file
                File productManifest = null;
                for (File file : unzippedFiles) {
                    if (file.getName().equalsIgnoreCase("xfdumanifest.xml")) {
                        productManifest = file;
                        break;
                    } else if (file.getName().equalsIgnoreCase("L1c_Manifest.xml")) {
                        productManifest = file;
                        break;
                    }
                }
                if (productManifest == null) {
                    throw new IllegalFileFormatException("input has no mainfest file.");
                }
                // ensure correct geo-coding, the default tie-point-geo-coding is BROKEN
                System.setProperty("s3tbx.reader.olci.pixelGeoCoding", "true");
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT);
                CalvalusLogger.getLogger().info("inputFormat = " + inputFormat);
                if (inputFormat != null && inputFormat.equals(FORMAT_NAME_SLSTRL1B_500m)) {
                    return ProductIO.readProduct(productManifest, "Sen3_SLSTRL1B_500m");
                } else if (inputFormat != null && inputFormat.equals(FORMAT_NAME_SLSTRL1B_1km)) {
                    return ProductIO.readProduct(productManifest, "Sen3_SLSTRL1B_1km");
                } else {
                    return ProductIO.readProduct(productManifest);
                }
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}

