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

package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.ErrorPredictor;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import javax.script.ScriptException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs the fire formatting grid mapper.
 *
 * @author thomas
 * @author marcop
 */
public class MerisGridMapper extends AbstractGridMapper {

    private final ErrorPredictor errorPredictor;
    private boolean maskUnmappablePixels;

    protected MerisGridMapper() {
        super(40, 40);
        try {
            errorPredictor = new ErrorPredictor();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        boolean computeBA = !paths[0].getName().equals("dummy");
        LOG.info(computeBA ? "Computing BA" : "Only computing coverage");
        maskUnmappablePixels = paths[0].getName().contains("v4.0.tif");
        if (maskUnmappablePixels) {
            LOG.info("v4.0 file; masking pixels which accidentally fall into unmappable LC class");
        }

        Product sourceProduct = null;
        Product lcProduct = null;
        List<File> srProducts = new ArrayList<>();
        if (computeBA) {
            File sourceProductFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
            sourceProduct = ProductIO.readProduct(sourceProductFile);

            File lcTile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
            lcProduct = ProductIO.readProduct(lcTile);
        }

        for (int i = 2; i < paths.length; i++) {
            File srProduct = CalvalusProductIO.copyFileToLocal(paths[i], context.getConfiguration());
            srProducts.add(srProduct);
        }

        setDataSource(new MerisDataSource(sourceProduct, lcProduct, srProducts));

        GridCells gridCells = computeGridCells(year, month);

        context.progress();

        context.write(new Text(String.format("%d-%02d-%s", year, month, getTile(paths[2].toString()))), gridCells);
        errorPredictor.dispose();
    }

    static String getTile(String path) {
        // path.toString() = hdfs://calvalus/calvalus/projects/fire/sr-fr-default-nc-classic/2008/v04h07/2008/2008-06-01-fire-nc/CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-06-01-v04h07.nc
        int startIndex = path.length() - 9;
        return path.substring(startIndex, startIndex + 6);
    }

    @Override
    protected void validate(float burnableFraction, List<double[]> baInLc, int targetGridCellIndex, double area) {
        // no burnable fraction computed for MERIS dataset
    }

    @Override
    protected int getLcClassesCount() {
        return LcRemapping.LC_CLASSES_COUNT;
    }

    @Override
    protected void addBaInLandCover(List<double[]> baInLc, int targetGridCellIndex, double burnedArea, int sourceLc) {
        for (int currentLcClass = 0; currentLcClass < getLcClassesCount(); currentLcClass++) {
            boolean inLcClass = LcRemapping.isInLcClass(currentLcClass + 1, sourceLc);
            baInLc.get(currentLcClass)[targetGridCellIndex] += inLcClass ? burnedArea : 0.0F;
        }
    }

    @Override
    protected float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double burnedPercentage) {
        // getting pixels for areas instead, see MerisGridMapper#predict
        return Float.NaN;
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
        try {
            float[] errors = errorPredictor.predictError(ba, areas);
            System.arraycopy(errors, 0, originalErrors, 0, errors.length);
        } catch (ScriptException e) {
            throw new RuntimeException(String.format("Unable to predict error from BA %s, areas %s", Arrays.toString(ba), Arrays.toString(areas)), e);
        }
    }
}
