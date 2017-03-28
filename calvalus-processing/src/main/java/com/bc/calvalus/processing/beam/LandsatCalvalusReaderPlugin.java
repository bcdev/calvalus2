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
import org.esa.snap.core.util.io.SnapFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.Locale;


/**
 * A reader for handing Landsat data on calvalus.
 * It unzips the products and open from the local file.
 */
public class LandsatCalvalusReaderPlugin implements ProductReaderPlugIn {

    private static final String FORMAT_15M = "CALVALUS-Landsat-15M";
    private static final String FORMAT_30M = "CALVALUS-Landsat-30M";
    private static final String FORMAT_MULTI = "CALVALUS-Landsat";

    private static final String L4_FILENAME_REGEX = "LT4\\d{13}\\w{3}\\d{2}";
    private static final String L5_FILENAME_REGEX = "LT5\\d{13}.{3}\\d{2}";
    private static final String L7_FILENAME_REGEX = "LE7\\d{13}.{3}\\d{2}";
    private static final String L8_FILENAME_REGEX = "L[O,T,C]8\\d{13}.{3}\\d{2}";
    private static final String COMPR_FILENAME_REGEX = "\\.(tar\\.gz|tgz|tar\\.bz|tbz|tar\\.bz2|tbz2|zip|ZIP)";

    private static final String[] FILENAME_PATTERNS = {
            L4_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L5_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L7_FILENAME_REGEX + COMPR_FILENAME_REGEX,
            L8_FILENAME_REGEX + COMPR_FILENAME_REGEX
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
                File[] unzippedFiles = CalvalusProductIO.uncompressArchiveToLocalDir(pathConfig.getPath(), configuration);

                // find manifest file
                File mtlFile = null;
                for (File file : unzippedFiles) {
                    if (file.getName().toLowerCase().endsWith("_mtl.txt")) {
                        mtlFile = file;
                        break;
                    }
                }
                if (mtlFile == null) {
                    throw new IllegalFileFormatException("input has no MTL file.");
                }
                String inputFormat = configuration.get(JobConfigNames.CALVALUS_INPUT_FORMAT, FORMAT_30M);
                System.out.println("inputFormat = " + inputFormat);

                switch (inputFormat) {
                    case FORMAT_15M:
                        return ProductIO.readProduct(mtlFile, "Landsat8GeoTIFF15m");
                    case FORMAT_30M:
                        return ProductIO.readProduct(mtlFile, "Landsat8GeoTIFF30m");
                    default:
                        return ProductIO.readProduct(mtlFile);
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

