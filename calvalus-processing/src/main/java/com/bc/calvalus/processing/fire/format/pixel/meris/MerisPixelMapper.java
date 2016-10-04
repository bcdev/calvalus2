/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.fire.format.pixel.meris;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * The fire formatting pixel mapper.
 *
 * @author thomas
 * @author marcop
 */
public class MerisPixelMapper extends Mapper<Text, FileSplit, Text, MerisPixelCell> {

    static final int RASTER_WIDTH = 3600;
    static final int RASTER_HEIGHT = 3600;

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));
        PixelVariableType variableType = PixelVariableType.valueOf(context.getConfiguration().get("variableType"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        boolean mosaicBA = !paths[0].getName().equals("dummy");
        LOG.info(mosaicBA ? "Mosaicking BA data" : "Only mosaicking burnable/non-burnable");

        File lcTile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(lcTile);

        MerisPixelCell pixelCell = new MerisPixelCell();
        pixelCell.values = new short[RASTER_WIDTH * RASTER_HEIGHT];

        int[] doy = new int[RASTER_WIDTH * RASTER_HEIGHT];
        int[] lc = new int[RASTER_WIDTH * RASTER_HEIGHT];
        int[] cl = new int[RASTER_WIDTH * RASTER_HEIGHT];

        lcProduct.getBand("lcclass").readPixels(0, 0, RASTER_WIDTH, RASTER_HEIGHT, lc);

        if (mosaicBA) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
            Product sourceProduct = ProductIO.readProduct(sourceProductFile);
            sourceProduct.getBand("band_1").readPixels(0, 0, RASTER_WIDTH, RASTER_HEIGHT, doy);

            switch (variableType) {
                case DAY_OF_YEAR:
                    for (int i = 0; i < lc.length; i++) {
                        if (doy[i] == 999) {
                            pixelCell.values[i] = 999;
                        } else if (LcRemapping.remap(lc[i]) == LcRemapping.INVALID_LC_CLASS) {
                            pixelCell.values[i] = 0;
                        } else {
                            pixelCell.values[i] = (short) doy[i];
                        }
                    }
                    break;
                case CONFIDENCE_LEVEL:
                    sourceProduct.getBand("band_2").readPixels(0, 0, RASTER_WIDTH, RASTER_HEIGHT, cl);
                    for (int i = 0; i < pixelCell.values.length; i++) {
                        if (doy[i] == 999) {
                            pixelCell.values[i] = 999;
                        } else if (LcRemapping.remap(lc[i]) == LcRemapping.INVALID_LC_CLASS) {
                            pixelCell.values[i] = 0;
                        } else {
                            pixelCell.values[i] = (short) (cl[i] / 100);
                        }
                    }
                    break;
                case LC_CLASS:
                    for (int i = 0; i < pixelCell.values.length; i++) {
                        if (doy[i] == 999) {
                            pixelCell.values[i] = 999;
                        } else if (doy[i] == 0) {
                            pixelCell.values[i] = 0;
                        } else {
                            pixelCell.values[i] = (short) LcRemapping.remap(lc[i]);
                        }
                    }
                    break;
            }
        } else {
            for (int i = 0; i < lc.length; i++) {
                pixelCell.values[i] = (short) (lc[i] == -46 ? 999 : 0);
            }
        }

        context.progress();
        String tile = mosaicBA ? getTileFromBA(paths[0]) : getTileFromLC(paths[1]);
        context.write(new Text(String.format("%d-%02d-%s", year, month, tile)), pixelCell);
    }

    private static String getTileFromBA(Path path) {
        int startIndex = path.toString().indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return path.toString().substring(startIndex, startIndex + 6);
    }

    private static String getTileFromLC(Path path) {
        return path.toString().substring(path.toString().length() - 9, path.toString().length() - 3);
    }

}
