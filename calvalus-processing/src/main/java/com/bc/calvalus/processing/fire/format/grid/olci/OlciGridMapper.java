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

package com.bc.calvalus.processing.fire.format.grid.olci;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Runs the fire formatting grid mapper for OLCI.
 *
 * @author thomas
 * @author marcop
 */
public class OlciGridMapper extends AbstractGridMapper {

    protected OlciGridMapper() {
        super(40, 40);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path compositesPath = new Path(fileSplit.getPath().getParent(), fileSplit.getPath().getName().replace("outputs", "composites"));
        Path lcMapPath = new Path(context.getConfiguration().get("calvalus.aux.lcMapPath"));

        LOG.info("Input split             : " + fileSplit);
        LOG.info("Corresponding composites: " + compositesPath);
        LOG.info("Previous year's LC Map  : " + lcMapPath);

        File outputTarFile = CalvalusProductIO.copyFileToLocal(fileSplit.getPath(), context.getConfiguration());
        File foaTarFile = CalvalusProductIO.copyFileToLocal(compositesPath, context.getConfiguration());
        File lcProductFile =
                (! (lcMapPath.getFileSystem(context.getConfiguration()) instanceof LocalFileSystem)) ?
                        CalvalusProductIO.copyFileToLocal(lcMapPath, context.getConfiguration()) :
                        lcMapPath.toString().startsWith("file:") ?
                                new File(lcMapPath.toString().substring(5)) :
                                new File(lcMapPath.toString());

        File[] outputsFiles = CommonUtils.untar(outputTarFile, "(.*Classification.*|.*Uncertainty.*)");
        File classificationFile = outputsFiles[0];
        File uncertaintyFile = outputsFiles[1];
        File foaFile = CommonUtils.untar(foaTarFile, ".*FractionOfObservedArea.*")[0];

        Product baProduct = ProductIO.readProduct(classificationFile);
        Product uncertaintyProduct = ProductIO.readProduct(uncertaintyFile);
        Product foaProduct = ProductIO.readProduct(foaFile);
        Product lcGlobal = ProductIO.readProduct(lcProductFile);

        // ba-outputs-h15v07-2019-08.tar.gz
        String tile = fileSplit.getPath().getName().substring("ba-outputs-".length(), "ba-outputs-h15v07".length());
        int h = new Integer(tile.substring(1, 3));
        int v = new Integer(tile.substring(4, 6));
        int x0 = h * 3600;
        int y0 = v * 3600;
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("bandNames", new String[]{"lccs_class"});
        parameters.put("region", new Rectangle(x0, y0, 3600, 3600));
        Product lcProduct = GPF.createProduct("Subset", parameters, lcGlobal);

        LOG.info(String.format("LC Map subset for tile %s created", tile));

        LOG.info(String.format("Classification product: %s", baProduct.getName()));
        LOG.info(String.format("Uncertainty product   : %s", uncertaintyProduct.getName()));
        LOG.info(String.format("Land cover product    : %s", lcProduct.getName()));

        String dateRanges = context.getConfiguration().get("calvalus.input.dateRanges");
        Matcher m = Pattern.compile(".*\\[.*(....-..-..).*:.*(....-..-..).*\\].*").matcher(dateRanges);
        if (! m.matches()) {
            throw new IllegalArgumentException(dateRanges + " is not a date range");
        }
        String timeCoverageStart = m.group(1);
        int year = Integer.parseInt(timeCoverageStart.substring(0,4));
        int month = Integer.parseInt(timeCoverageStart.substring(5,7));

        setDataSource(new OlciDataSource(baProduct, foaProduct, uncertaintyProduct, lcProduct));
        GridCells gridCells = computeGridCells(year, month, context);
        context.progress();
        context.write(new Text(String.format("%d-%02d-%s", year, month, tile)), gridCells);

        LOG.info(String.format("Grid cells for %d-%02d-%s streamed", year, month, tile));
    }

    @Override
    protected void validate(float burnableFraction, List<double[]> baInLc, int targetGridCellIndex, double area) {
        double lcAreaSum = 0.0F;
        for (double[] baValues : baInLc) {
            lcAreaSum += baValues[targetGridCellIndex];
        }
        float lcAreaSumFraction = getFraction(lcAreaSum, area);
        if (lcAreaSumFraction > burnableFraction * 1.05) {
            throw new IllegalStateException("lcAreaSumFraction (" + lcAreaSumFraction + ") > burnableAreaFraction * 1.05 (" + burnableFraction * 1.2 + ") in first half");
        }
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
        // Mask all pixels with value 255 in the confidence level (corresponding to the pixels not observed or non-burnable in the JD layer)
        // From the remaining pixels, reassign all values of 0 to 1

        double[] probabilityOfBurnMasked = Arrays.stream(probabilityOfBurn)
// 2019-11-04 looks strange, the unburnable pixels are 0, and they shall be filtered, not shifted
//                .map(d -> d == 0 ? 1.0 : d)
                .filter(d -> d <= 100.0 && d >= 1.0)
                .toArray();

        // n is the number of pixels in the 0.25º cell that were not masked
        int n = probabilityOfBurnMasked.length;

        if (n == 1) {
            return (float) (gridCellArea / probabilityOfBurn.length);
        }

        // pb_i = value of confidence level of pixel /100
        // Var_c = sum (pb_i*(1-pb_i)
        double var_c = Arrays.stream(probabilityOfBurnMasked)
                .map(d -> d / 100.0)
                .map(pb_i -> (pb_i * (1.0 - pb_i)))
                .sum();

        // SE = sqr(var_c*(n/(n-1))) * pixel area
        // pixel area is the average area of a pixel contributing to the grid cell.
        return (float) (Math.sqrt(var_c * (n / (n - 1.0))) * (gridCellArea / probabilityOfBurn.length));
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }
}
