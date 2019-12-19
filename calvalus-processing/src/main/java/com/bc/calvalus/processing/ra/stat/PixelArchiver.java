/*
 * Copyright (C) 2017 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.ra.stat;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import org.esa.snap.dataio.netcdf.nc.NFileWriteable;
import org.esa.snap.dataio.netcdf.nc.NVariable;
import org.esa.snap.dataio.netcdf.nc.NWritableFactory;
import org.esa.snap.dataio.netcdf.util.NetcdfFileOpener;
import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Writes all extracted pixels to a netCDF file per region and band
 */
public class PixelArchiver {

    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final DateFormat DATE_FORMAT = DateUtils.createDateFormat("yyyy_MM_dd_HH_mm_ss");

    private final String regionName;
    private final String[] bandNames;
    private final Map<String, List<NcContent>> files;
    private long time;
    private int numObs;
    private String productName;
    private final Accumulator[] bandAccus;

    public PixelArchiver(String regionName, String... bandNames) {
        this.regionName = regionName;
        this.bandNames = bandNames;
        this.files = new HashMap<>();
        for (String bandName : bandNames) {
            files.put(bandName, new ArrayList<>());
        }
        this.time = -1;
        this.numObs = 0;
        this.bandAccus = new Accumulator[bandNames.length];
        for (int i = 0; i < bandNames.length; i++) {
            this.bandAccus[i] = new Accumulator();
        }
    }

    public void addProductPixels(long time, int numObs, float[][] samples, String productName) throws IOException {
        this.time = time;
        this.numObs += numObs;
        this.productName = productName;
        for (int i = 0; i < bandNames.length; i++) {
            bandAccus[i].accumulate(samples[i]);   
        }
    }
    public void writeTempNetcdf() throws IOException {
        if (this.time == -1 || bandAccus[0].getValues().length == 0) {
            return;
        }
        String timeString = DATE_FORMAT.format(new Date(time));
        for (int i = 0; i < bandNames.length; i++) {
            String bandName = bandNames[i];
            float[] bandSamples = bandAccus[i].getValues();
            int numSamples = bandSamples.length;

            String tmpFilename = "tmp.values-" + regionName + "-" + bandName + "-" + timeString +".nc";
            String varName = bandName + "_" + timeString;
            String dimName = "dim_" + timeString;

            NFileWriteable nFileWriteable = NWritableFactory.create(tmpFilename, "netcdf4");
            nFileWriteable.addDimension(dimName, numSamples);
            NVariable nVariable = nFileWriteable.addVariable(varName, DataType.FLOAT, null, dimName);
            nVariable.addAttribute("product_name", productName);
            nVariable.addAttribute("num_obs", numObs);
            nFileWriteable.create();
            nVariable.writeFully(Array.factory(DataType.FLOAT, new int[]{bandSamples.length}, bandSamples));
            nFileWriteable.close();
            
            bandAccus[i].clear();

            files.get(bandName).add(new NcContent(tmpFilename, timeString, productName, numObs, numSamples));
        }
        this.numObs = 0;
        this.time = -1;
    }

    public File[] createMergedNetcdf() throws IOException {
        List<File> mergedFiles = new ArrayList<>();
        for (String bandName : bandNames) {
            LOG.info("merging value files:");
            List<NcContent> tmpFiles = this.files.get(bandName);
            String finalFilename = "values-" + regionName + "-" + bandName + ".nc";
            NFileWriteable nFileWriteable = NWritableFactory.create(finalFilename, "netcdf4");
            for (NcContent tmpNcContent : tmpFiles) {
                String varName = bandName + "_" + tmpNcContent.timeString;
                String dimName = "dim_" + tmpNcContent.timeString;

                nFileWriteable.addDimension(dimName, tmpNcContent.numSamples);
                NVariable nVariable = nFileWriteable.addVariable(varName, DataType.FLOAT, null, dimName);
                nVariable.addAttribute("product_name", tmpNcContent.productName);
                nVariable.addAttribute("num_obs", tmpNcContent.numObs);
            }
            nFileWriteable.create();
            for (NcContent tmpNcContent : tmpFiles) {
                LOG.info("    " + tmpNcContent.filename);
                String varName = bandName + "_" + tmpNcContent.timeString;
                NetcdfFile ncInput = NetcdfFileOpener.open(tmpNcContent.filename);
                if (ncInput != null) {
                    try {
                        Variable variable = ncInput.findVariable(varName);
                        Array data = variable.read();
                        nFileWriteable.findVariable(varName).writeFully(data);
                    } finally {
                        ncInput.close();
                    }
                }
            }
            LOG.info("  ==> " + finalFilename);
            nFileWriteable.close();

            mergedFiles.add(new File(finalFilename));
        }
        return mergedFiles.toArray(new File[0]);
    }

    private static class NcContent {

        private final String filename;
        private final String timeString;
        private final String productName;
        private final int numObs;
        private final int numSamples;

        private NcContent(String filename, String timeString, String productName, int numObs, int numSamples) {
            this.filename = filename;
            this.timeString = timeString;
            this.productName = productName;
            this.numObs = numObs;
            this.numSamples = numSamples;
        }
    }

    public static void main(String[] args) throws ParseException, IOException {
        PixelArchiver pixelArchiver = new PixelArchiver("reg", "b1", "b2");
        long time1 = DATE_FORMAT.parse("2010_01_01_11_11_11").getTime();
        long time2 = DATE_FORMAT.parse("2010_01_02_11_11_11").getTime();
        float[][] samples1 = new float[][]{{1, 2, 3, 4, 5, 6, 7, 8, 9}, {3, 6, Float.NaN}};
        float[][] samples2 = new float[][]{{2, 3, 4, 5, 76, 2, 3, 4, 5, 5}, {3, 6, 7}};
        pixelArchiver.addProductPixels(time1, 42, samples1, "p1");
        pixelArchiver.addProductPixels(time1, 11, samples2, "p1");
        pixelArchiver.writeTempNetcdf();
        pixelArchiver.addProductPixels(time2, 96, samples2, "p2");
        pixelArchiver.writeTempNetcdf();
        File[] mergedNetcdf = pixelArchiver.createMergedNetcdf();
        System.out.println("mergedNetcdf = " + Arrays.toString(mergedNetcdf));
    }
}
