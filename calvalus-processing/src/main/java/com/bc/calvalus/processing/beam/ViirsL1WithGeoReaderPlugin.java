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

import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * This reader plugin handles VIIRS L1
 * V2016215000000.GEO-M_SNPP.nc
 * V2016215000000.L1A_SNPP.nc
 */
public class ViirsL1WithGeoReaderPlugin implements ProductReaderPlugIn {

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            String filename = pathConfig.getPath().getName();
            if (filename.matches("V\\d{13}\\.L1A_SNPP.nc")) {
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
        return new ViirsL1WithGeoReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"VIIRSN_WITH_GEO"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".nc"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "VIIRSN L1A with GEO file";
    }

    @Override
    public SnapFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    private static class ViirsL1WithGeoReader extends AbstractProductReader {

        public ViirsL1WithGeoReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                File localFile = CalvalusProductIO.copyFileToLocal(pathConfig.getPath(), pathConfig.getConfiguration());
                copyViirsGeoFile(pathConfig);
                //return ProductIO.readProduct(localFile, "VIIRS");  TODO which reader reads L1A+GEO?
                return ProductIO.readProduct(localFile);
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        private static void copyViirsGeoFile(PathConfiguration pathConfig) throws IOException {
            final Path geoPath = new Path(pathConfig.getPath().getParent(), pathConfig.getPath().getName().replaceAll("L1A", "GEO-M"));
            FileSystem fs = pathConfig.getPath().getFileSystem(pathConfig.getConfiguration());
            FileStatus fileStatus = fs.getFileStatus(geoPath);
            try {
                File localGeoFile = new File(".", geoPath.getName());
                CalvalusProductIO.copyFileToLocal(geoPath, localGeoFile, pathConfig.getConfiguration());
                System.out.println("localGeoFile: " + localGeoFile);
            } catch (FileNotFoundException ex) {
                throw new IllegalArgumentException("No GEO file found for " + pathConfig.getPath());
            }
        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }
}


