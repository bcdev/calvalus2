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

package com.bc.calvalus.processing.mosaic;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces list of spatial tiles to a temporal tile.
 *
 * @author Marco Zuehlke
 */
public class MosaicReducer extends Reducer<TileIndexWritable, TileDataWritable, TileIndexWritable, TileDataWritable> {


    @Override
    protected void reduce(TileIndexWritable tileIndex, Iterable<TileDataWritable> spatialTiles, Context context) throws IOException, InterruptedException {

        float[][] temporalSampleValues = null;
        int counter = 0;

        for (TileDataWritable spatialTile : spatialTiles) {
            float[][] samples = spatialTile.getSamples();
            if (temporalSampleValues == null) {
                temporalSampleValues = new float[samples.length][samples[0].length];
            }
            for (int i = 0; i < samples.length; i++) {
                for (int j = 0; j < samples[i].length; j++) {
                    temporalSampleValues[i][j] += samples[i][j];
                }
            }
            counter++;
        }
        if (counter > 0) {
            for (int i = 0; i < temporalSampleValues.length; i++) {
                for (int j = 0; j < temporalSampleValues[i].length; j++) {
                    temporalSampleValues[i][j] /= counter;
                }
            }
        }
        TileDataWritable value = new TileDataWritable(370, 370, temporalSampleValues);
        context.write(tileIndex, value);
    }
}
