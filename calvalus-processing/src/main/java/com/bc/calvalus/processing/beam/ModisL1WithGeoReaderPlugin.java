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
import org.esa.beam.framework.dataio.AbstractProductReader;
import org.esa.beam.framework.dataio.DecodeQualification;
import org.esa.beam.framework.dataio.IllegalFileFormatException;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.dataio.ProductReaderPlugIn;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;
import org.esa.beam.util.io.BeamFileFilter;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * This reader plugin handles MODIS
 * A2003001000000.L1A_LAC
 * A2003001000000.L1B_LAC
 * numDigits = 4 + 3 + 6 = 13
 */
public class ModisL1WithGeoReaderPlugin implements ProductReaderPlugIn {

    private static final String AUX_GEO_PATHES = "calvalus.auxdata.modis_geo";

    @Override
    public DecodeQualification getDecodeQualification(Object input) {
        if (input instanceof PathConfiguration) {
            PathConfiguration pathConfig = (PathConfiguration) input;
            if (pathConfig.getConfiguration().get(AUX_GEO_PATHES) != null) {
                String filename = pathConfig.getPath().getName();
                if (filename.matches("A\\d{13}\\.L1[AB]_LAC")) {
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
        return new ModisL1WithGeoReader(this);
    }

    @Override
    public String[] getFormatNames() {
        return new String[]{"MODISA_WITH_GEO"};
    }

    @Override
    public String[] getDefaultFileExtensions() {
        return new String[]{".none"};
    }

    @Override
    public String getDescription(Locale locale) {
        return "MODISA with GEO Files";
    }

    @Override
    public BeamFileFilter getProductFileFilter() {
        return null; // only used in UI
    }

    private static class ModisL1WithGeoReader extends AbstractProductReader {

        public ModisL1WithGeoReader(ProductReaderPlugIn productReaderPlugIn) {
            super(productReaderPlugIn);
        }

        @Override
        protected Product readProductNodesImpl() throws IOException {
            Object input = getInput();
            if (input instanceof PathConfiguration) {
                PathConfiguration pathConfig = (PathConfiguration) input;
                File localFile = CalvalusProductIO.copyFileToLocal(pathConfig.getPath(), pathConfig.getConfiguration());
                copyModisGeoFile(pathConfig);
                return ProductIO.readProduct(localFile);
            } else {
                throw new IllegalFileFormatException("input is not of the correct type.");
            }
        }

        private static void copyModisGeoFile(PathConfiguration pathConfig) throws IOException {
            String filename = pathConfig.getPath().getName();
            String year = filename.substring(1, 5);
            String doy = filename.substring(5, 8);
            String granule = filename.substring(0, 14);

            Calendar calendar = ProductData.UTC.createCalendar();
            calendar.set(Calendar.YEAR, Integer.parseInt(year));
            calendar.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));
            Date productDay = calendar.getTime();

            String[] auxGeoPathes = pathConfig.getConfiguration().getStrings(AUX_GEO_PATHES);
            if (auxGeoPathes == null) {
                throw new IllegalStateException("ModisL1WithGeoReaderPlugin NOT enabled");
            }
            for (String auxGeoPath : auxGeoPathes) {
                DateFormat dateFormat = ProductData.UTC.createDateFormat(auxGeoPath);
                String geoGlob = dateFormat.format(productDay) + granule + ".GEO*";

                FileSystem fileSystem = FileSystem.get(pathConfig.getConfiguration());
                FileStatus[] fileStatuses = fileSystem.globStatus(new Path(geoGlob));
                if (fileStatuses != null && fileStatuses.length > 0) {
                    // take the first one
                    Path geoPath = fileStatuses[0].getPath();
                    File localGeoFile =  new File(".", granule + ".GEO");
                    CalvalusProductIO.copyFileToLocal(geoPath, localGeoFile, pathConfig.getConfiguration());
                    System.out.println("localGeoFile: " + localGeoFile);
                    return;
                }
            }
            throw new IllegalArgumentException("No GEO file found");

        }

        @Override
        protected void readBandRasterDataImpl(int i, int i2, int i3, int i4, int i5, int i6, Band band, int i7, int i8, int i9, int i10, ProductData productData, ProgressMonitor progressMonitor) throws IOException {
            throw new IllegalStateException("Should not be called");
        }
    }

    public static void main(String[] args) throws IOException {
//        Path l1bPath = new Path("hdfs://master00:9000/calvalus/eodata/MODIS_L1B/OBPG/2002/06/26/A2002177001500.L1B_LAC");
        Path l1bPath = new Path("A2002177001500");
        Configuration config = new Configuration(false);
        config.set("fs.defaultFS", "hdfs://master00:9000");
        config.set(AUX_GEO_PATHES, "'/calvalus/eodata/MODISA_GEO/v1/'yyyy'/'MM'/'dd'/'");
        PathConfiguration pathConfig = new PathConfiguration(l1bPath, config);

//        CalvalusProductIO.copyFileToLocal(l1bPath, pathConfig.getConfiguration());
        ModisL1WithGeoReader.copyModisGeoFile(pathConfig);
    }
}
