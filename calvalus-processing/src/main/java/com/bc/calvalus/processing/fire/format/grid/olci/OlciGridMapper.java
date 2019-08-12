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

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractGridMapper;
import com.bc.calvalus.processing.fire.format.grid.GridCells;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        if (inputSplit.getPaths().length != 3) {
            throw new IllegalStateException("3 input paths needed: classification, fraction of observed area, LandCover");
        }
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));


        File outputTarFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
        File foaTarFile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        File lcProductFile = CalvalusProductIO.copyFileToLocal(paths[2], context.getConfiguration());

        File[] output = untar(outputTarFile, "(.*Classification.*|.*Uncertainty.*)");
        File classificationFile = output[0];
        File uncertaintyFile = output[1];
        File foa = untar(foaTarFile, ".*FractionOfObservedArea.*")[0];

        Product baProduct = ProductIO.readProduct(classificationFile);
        Product foaProduct = ProductIO.readProduct(foa);
        Product uncertaintyProduct = ProductIO.readProduct(uncertaintyFile);
        Product lcProduct = ProductIO.readProduct(lcProductFile);

        setDataSource(new OlciDataSource(baProduct, foaProduct, uncertaintyProduct, lcProduct));

        GridCells gridCells = computeGridCells(year, month, context);

        context.progress();

        context.write(new Text(String.format("%d-%02d-%s", year, month, getTile(paths[2].toString()))), gridCells);
    }

    static File[] untar(File tarFile, String filterRegEx) {
        return untar(tarFile, filterRegEx, null);
    }

    static File[] untar(File tarFile, String filterRegEx, List<String> newDirs) {
        List<File> result = new ArrayList<>();
        try (FileInputStream in = new FileInputStream(tarFile);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(in);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

            TarArchiveEntry entry;

            while ((entry = (TarArchiveEntry) tarIn.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    boolean created = new File(entry.getName()).mkdirs();
                    if (!created) {
                        throw new IOException(String.format("Unable to create directory '%s' during extraction of contents of archive: '", entry.getName()));
                    }
                } else {
                    int count;
                    byte[] data = new byte[1024];
                    if (!entry.getName().matches(filterRegEx)) {
                        continue;
                    }
                    int lastIndex = entry.getName().lastIndexOf("/");
                    boolean created = new File(entry.getName().substring(0, lastIndex)).mkdirs();
                    if (created && newDirs != null) {
                        Collections.addAll(newDirs, entry.getName().substring(0, lastIndex).split("/"));
                    }
                    FileOutputStream fos = new FileOutputStream(entry.getName(), false);
                    try (BufferedOutputStream dest = new BufferedOutputStream(fos, 1024)) {
                        while ((count = tarIn.read(data, 0, 1024)) != -1) {
                            dest.write(data, 0, count);
                        }
                    }
                    result.add(new File(entry.getName()));
                }
            }

        } catch (IOException e) {
            throw new IllegalStateException("Unable to extract tar archive '" + tarFile + "'", e);
        }
        File[] untarredFiles = result.toArray(new File[0]);
        Arrays.sort(untarredFiles, Comparator.comparing(File::getName));
        return untarredFiles;
    }

    static String getTile(String path) {
        // sample: path.toString() = hdfs://calvalus/calvalus/.../lc-h31v10.nc
        int startIndex = path.lastIndexOf("/");
        return path.substring(startIndex + 4, startIndex + 10);
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
                .map(d -> d == 0 ? 1.0 : d)
                .filter(d -> d <= 100.0 && d >= 1.0)
                .toArray();

        // n is the number of pixels in the 0.25º cell that were not masked
        int n = probabilityOfBurnMasked.length;

        if (n == 1) {
            return (float) gridCellArea;
        }

        // pb_i = value of confidence level of pixel /100
        double[] pb = Arrays.stream(probabilityOfBurnMasked).map(d -> d / 100.0).toArray();

        // Var_c = sum (pb_i*(1-pb_i)
        double var_c = Arrays.stream(pb)
                .map(pb_i -> (pb_i * (1.0 - pb_i)))
                .sum();

        // SE = sqr(var_c*(n/(n-1))) * pixel area
        // pixel area is the sum of the area of the pixels.
        return (float) (Math.sqrt(var_c * (n / (n - 1.0))) * gridCellArea);
    }

    @Override
    protected void predict(double[] ba, double[] areas, float[] originalErrors) {
    }
}
