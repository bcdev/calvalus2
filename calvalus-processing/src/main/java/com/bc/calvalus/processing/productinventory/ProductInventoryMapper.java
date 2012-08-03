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

package com.bc.calvalus.processing.productinventory;

import com.bc.calvalus.processing.beam.ProcessorAdapter;
import com.bc.calvalus.processing.beam.ProcessorAdapterFactory;
import com.bc.calvalus.processing.shellexec.ProcessorException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.Band;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.TiePointGrid;
import org.esa.beam.framework.gpf.OperatorException;
import org.esa.beam.gpf.operators.standard.BandMathsOp;

import java.io.IOException;

/**
 * A mapper that checks products for validity and generates an inventory entry.
 *
 * @author MarcoZ
 */
public class ProductInventoryMapper extends Mapper<NullWritable, NullWritable, Text /*product name*/, Text /*inventory entry CSV*/> {

    @Override
    public void run(Context context) throws IOException, InterruptedException, ProcessorException {
        final FileSplit split = (FileSplit) context.getInputSplit();
        // parse request
        Path inputPath = split.getPath();

        if (inputPath.getName().endsWith("N1") && split.getLength() <= 12029L) {
            report(context, "Input N1 file to small", inputPath);
            return;
        }

        ProcessorAdapter processorAdapter = ProcessorAdapterFactory.create(context);
        Product product = null;
        try {
            product = processorAdapter.getProcessedProduct();
            if (productHasEmptyTiepoints(product)) {
                report(context, product, false, "Product has empty tie-points", inputPath);
            } else if (productHasEmptyLatLonLines(product)) {
                report(context, product, false, "Product has empty lat/lon lines", inputPath);
// disabled does produce a lot of false positives
//            } else if (productHasSuspectLines(product)) {
//                report(context, product, false, "Product has suspect lines", inputPath);
            } else {
                report(context, product, true, "Good", inputPath);
            }
        } catch (Exception exception) {
            if (product != null) {
                report(context, product, false, "Failed to read product: " + exception.getMessage(), inputPath);
            } else {
                report(context, "Failed to read product: " + exception.getMessage(), inputPath);
            }
        } finally {
            processorAdapter.dispose();
        }
    }

    private void report(Context context, String message, Path path) throws IOException, InterruptedException {
        report(context, ProductInventoryEntry.createEmpty(message), path);
    }

    private void report(Context context, Product product, boolean good, String message, Path path) throws IOException, InterruptedException {
        ProductInventoryEntry entry;
        if (good) {
            entry = ProductInventoryEntry.createForGoodProduct(product, message);
        } else {
            entry = ProductInventoryEntry.createForBadProduct(product, message);
        }
        report(context, entry, path);
    }

    private void report(Context context, ProductInventoryEntry entry, Path path) throws IOException, InterruptedException {
        context.write(new Text(path.getName()), new Text(entry.toCSVString()));
        context.getCounter("Products", entry.getMessage()).increment(1);
    }

    private static boolean productHasSuspectLines(Product product) throws IOException {
        BandMathsOp mathop1 = null;
        BandMathsOp mathop2 = null;
        try {
            mathop1 = BandMathsOp.createBooleanExpressionBand("l1_flags.INVALID", product);
            Band invalid = mathop1.getTargetProduct().getBandAt(0);

            mathop2 = BandMathsOp.createBooleanExpressionBand("l1_flags.SUSPECT", product);
            Band suspect = mathop2.getTargetProduct().getBandAt(0);

            int width = product.getSceneRasterWidth();
            int height = product.getSceneRasterHeight();

            int[] invalidFlags = new int[width];
            int[] suspectFlags = new int[width];
            for (int y = 0; y < height; y++) {
                invalid.readPixels(0, y, width, 1, invalidFlags);
                suspect.readPixels(0, y, width, 1, suspectFlags);
                if (isWholeLineSuspect(invalidFlags, suspectFlags)) {
                    return true;
                }
            }
            return false;
        } catch (OperatorException ignore) {
            return false;
        } finally {
            if (mathop1 != null) {
                mathop1.dispose();
            }
            if (mathop2 != null) {
                mathop2.dispose();
            }
        }
    }

    static boolean isWholeLineSuspect(int[] invalidFlags, int[] suspectFlags) {
        int state = 0;
        for (int i = 0; i < invalidFlags.length; i++) {
            boolean isInvalid = invalidFlags[i] != 0;
            boolean isSuspect = suspectFlags[i] != 0;
            if ((state == 0 || state == 1) && isInvalid && !isSuspect) {
                state = 1;
            } else if ((state == 1 || state == 2) && !isInvalid && isSuspect) {
                state = 2;
            } else if (state == 3 && i == (invalidFlags.length-1)) {
                return true;
            } else if ((state == 2 || state == 3) && isInvalid && !isSuspect) {
                state = 3;
            } else {
                return false;
            }
        }
        return false;
    }

    private static boolean productHasEmptyTiepoints(Product sourceProduct) {
        // "AMORGOS" can produce products that are corrupted.
        // All tie point grids contain only zeros, check the first one,
        // if the product has one.
        TiePointGrid[] tiePointGrids = sourceProduct.getTiePointGrids();
        if (tiePointGrids != null && tiePointGrids.length > 0) {
            TiePointGrid firstGrid = tiePointGrids[0];
            float[] tiePoints = firstGrid.getTiePoints();
            for (float tiePoint : tiePoints) {
                if (tiePoint != 0.0f) {
                    return false;
                }
            }
            // all values are zero
            return true;
        }
        return false;
    }

    private static boolean productHasEmptyLatLonLines(Product sourceProduct) {
        TiePointGrid latitude = sourceProduct.getTiePointGrid("latitude");
        TiePointGrid longitude = sourceProduct.getTiePointGrid("longitude");
        if (latitude == null || longitude == null) {
            return false;
        }
        float[] latData = (float[]) latitude.getDataElems();
        float[] lonData = (float[]) longitude.getDataElems();

        int width = latitude.getRasterWidth();
        int height = latitude.getRasterHeight();

        for (int y = 0; y < height; y++) {
            if (isLineZero(latData, width, y) && isLineZero(lonData, width, y)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isLineZero(float[] floatData, int width, int y) {
        for (int x = 0; x < width; x++) {
            if (floatData[(y * width + x)] != 0) {
                return false;
            }
        }
        return true;
    }
}
