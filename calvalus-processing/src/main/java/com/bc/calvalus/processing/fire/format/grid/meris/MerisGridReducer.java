package com.bc.calvalus.processing.fire.format.grid.meris;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.grid.GridReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import ucar.ma2.InvalidRangeException;

import java.io.File;
import java.io.IOException;

public class MerisGridReducer extends GridReducer {

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        prepareTargetProducts(context);

        String year = context.getConfiguration().get("calvalus.year");
        String month = context.getConfiguration().get("calvalus.month");
        String basePath = "hdfs://calvalus/calvalus/projects/fire/aux/psd-grid-for-oaf/";
        String firstHalfFilename = year + month + "07-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
        String secondHalfFilename = year + month + "22-ESACCI-L4_FIRE-BA-MERIS-fv04.1.nc";
        Path firstHalfGridPath = new Path(basePath, firstHalfFilename);
        Path secondHalfGridPath = new Path(basePath, secondHalfFilename);
        File firstHalfGridFile = CalvalusProductIO.copyFileToLocal(firstHalfGridPath, context.getConfiguration());
        File secondHalfGridFile = CalvalusProductIO.copyFileToLocal(secondHalfGridPath, context.getConfiguration());

        Product firstHalfGridProduct = ProductIO.readProduct(firstHalfGridFile);
        Product secondHalfGridProduct = ProductIO.readProduct(secondHalfGridFile);

        float[] buffer = new float[40 * 40];

        try {
            for (int y = 0; y < 18; y++) {
                for (int x = 0; x < 36; x++) {
                    firstHalfGridProduct.getBand("observed_area_fraction").readPixels(x * 40, y * 40, 40, 40, buffer);
                    writeFloatChunk(x * 40, y * 40, ncFirst, "observed_area_fraction", buffer);
                    secondHalfGridProduct.getBand("observed_area_fraction").readPixels(x * 40, y * 40, 40, 40, buffer);
                    writeFloatChunk(x * 40, y * 40, ncSecond, "observed_area_fraction", buffer);
                }
            }
        } catch (InvalidRangeException e) {
            throw new IOException(e);
        }
    }
}
