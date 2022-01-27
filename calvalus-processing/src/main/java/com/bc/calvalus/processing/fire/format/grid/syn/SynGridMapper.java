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

package com.bc.calvalus.processing.fire.format.grid.syn;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import com.bc.calvalus.processing.fire.format.grid.olci.OlciSynDataSource;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Runs the fire formatting grid mapper for OLCI.
 *
 * @author thomas
 * @author marcop
 */
public class SynGridMapper extends AbstractGridMapper {

    protected SynGridMapper() {
        super(40, 40);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        FileSplit fileSplit = (FileSplit) context.getInputSplit();

        Path lcMapPath = new Path(context.getConfiguration().get("calvalus.aux.lcMapPath"));

        LOG.info("Input split             : " + fileSplit);
        LOG.info("Previous year's LC Map  : " + lcMapPath);

        File inputTarFile = CalvalusProductIO.copyFileToLocal(fileSplit.getPath(), context.getConfiguration());
        File lcProductFile =
                (! (lcMapPath.getFileSystem(context.getConfiguration()) instanceof LocalFileSystem)) ?
                        CalvalusProductIO.copyFileToLocal(lcMapPath, context.getConfiguration()) :
                        lcMapPath.toString().startsWith("file:") ?
                                new File(lcMapPath.toString().substring(5)) :
                                new File(lcMapPath.toString());

        File[] outputsFiles = CommonUtils.untar(inputTarFile, "(.*Classification.*|.*BurnProbabilityError.*|.*FractionOfObservedArea.*)");
        File uncertaintyFile = outputsFiles[0];
        File classificationFile = outputsFiles[1];
        File foaFile = outputsFiles[2];

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

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        setDataSource(new OlciSynDataSource(baProduct, foaProduct, uncertaintyProduct, lcProduct));
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
    protected float getErrorPerPixel(double[] probabilityOfBurn, double gridCellArea, double[] areas, double burnedPercentage) {
        int[] indices = Arrays.stream(probabilityOfBurn).mapToInt(value -> value > 0 ? 1 : 0).toArray();
        double[] u = Arrays.stream(probabilityOfBurn).filter(value -> value > 0).toArray();
        int N = u.length;
        double[][] Sx = new double[N][N];
        for (int i = 0; i < Sx.length; i++) {
            Sx[i] = new double[N];
            for (int j = 0; j < Sx[i].length; j++) {
                Sx[i][j] = u[i] * u[j];
            }
        }

        double[] C = getC(areas, indices, N);
        double[][] C_T = transpose(C);
        double[] V = getDotProduct(C, Sx);
        double result = getDotProduct(V, C_T)[0];

        return (float) Math.sqrt(result);
    }

    static double[][] transpose(double[] C) {
        double[][] C_T = new double[1][];
        C_T[0] = C;
        return C_T;
    }

    static double[] getDotProduct(double[] C, double[][] Sx) {
        double[] V = new double[Sx.length];
        for (int c = 0; c < V.length; c++) {
            for (int r = 0 ; r < C.length; r++) {
                V[c] += C[r] * Sx[c][r];
            }
        }
        return V;
    }

    private static double[] getC(double[] areas, int[] indices, int N) {
        double[] C = new double[N];
        int j = 0;
        for (int k = 0; k < indices.length; k++) {
            if (indices[k] == 1) {
                C[j++] = areas[k];
            }
        }
        return C;
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }
}
