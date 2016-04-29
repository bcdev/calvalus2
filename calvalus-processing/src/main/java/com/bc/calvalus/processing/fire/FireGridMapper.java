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

package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.*;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import javax.script.ScriptException;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * Runs the fire formatting grid mapper.
 *
 * @author thomas
 * @author marcop
 */
public class FireGridMapper extends Mapper<Text, FileSplit, Text, GridCell> {

    static final int TARGET_RASTER_WIDTH = 40;
    static final int TARGET_RASTER_HEIGHT = 40;
    static final int LC_CLASSES_COUNT = 18;
    static final int NO_DATA = -1;
    static final int NO_AREA = 0;

    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        int doyFirstHalf = Year.of(year).atMonth(month).atDay(7).getDayOfYear();
        int doySecondHalf = Year.of(year).atMonth(month).atDay(22).getDayOfYear();

        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        Path[] paths = inputSplit.getPaths();
        LOG.info("paths=" + Arrays.toString(paths));

        String tile = getTile(paths[0]);
        File centerSourceProductFile = CalvalusProductIO.copyFileToLocal(paths[0], context.getConfiguration());
        Product centerSourceProduct = ProductIO.readProduct(centerSourceProductFile);

        File lcTile = CalvalusProductIO.copyFileToLocal(paths[1], context.getConfiguration());
        Product lcProduct = ProductIO.readProduct(lcTile);

        Product targetProduct = createTargetProduct(tile);

        FireGridDataSource dataSource = new FireGridDataSourceImpl(centerSourceProduct, lcProduct);
        ErrorPredictor errorPredictor = new ErrorPredictor();
        dataSource.setDoyFirstOfMonth(doyFirstOfMonth);
        dataSource.setDoyLastOfMonth(doyLastOfMonth);
        dataSource.setDoyFirstHalf(doyFirstHalf);
        dataSource.setDoySecondHalf(doySecondHalf);

        Band burnedAreaFirstHalf = targetProduct.addBand("burned_area_first_half", ProductData.TYPE_FLOAT32);
        Band burnedAreaSecondHalf = targetProduct.addBand("burned_area_second_half", ProductData.TYPE_FLOAT32);
        Band patchNumberFirstHalf = targetProduct.addBand("patch_number_first_half", ProductData.TYPE_INT32);
        Band patchNumberSecondHalf = targetProduct.addBand("patch_number_second_half", ProductData.TYPE_INT32);
        Band errorFirstHalf = targetProduct.addBand("error_first_half", ProductData.TYPE_FLOAT32);
        Band errorSecondHalf = targetProduct.addBand("error_second_half", ProductData.TYPE_FLOAT32);

        SourceData data = new SourceData();
        double[] areas = new double[TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT];

        PixelPos pixelPos = new PixelPos();
        GeoPos geoPos = new GeoPos();
        ProductData.Float baFirstHalfRasterData = new ProductData.Float(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT);
        burnedAreaFirstHalf.setRasterData(baFirstHalfRasterData);
        ProductData.Float baSecondHalfRasterData = new ProductData.Float(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT);
        burnedAreaSecondHalf.setRasterData(baSecondHalfRasterData);
        ProductData.Int patchNumberFirstHalfData = new ProductData.Int(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT);
        patchNumberFirstHalf.setRasterData(patchNumberFirstHalfData);
        ProductData.Int patchNumberSecondHalfData = new ProductData.Int(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT);
        patchNumberSecondHalf.setRasterData(patchNumberSecondHalfData);

        List<float[]> baInLcFirstHalf = new ArrayList<>();
        List<float[]> baInLcSecondHalf = new ArrayList<>();
        for (int c = 1; c <= LC_CLASSES_COUNT; c++) {
            baInLcFirstHalf.add(new float[TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT]);
            baInLcSecondHalf.add(new float[TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT]);
        }

        context.progress();

        int targetPixelIndex = 0;
        for (int y = 0; y < TARGET_RASTER_HEIGHT; y++) {
            LOG.info(String.format("Processing line %d of target raster.", y));
            for (int x = 0; x < TARGET_RASTER_WIDTH; x++) {
                data.reset();
                pixelPos.x = x;
                pixelPos.y = y;
                targetProduct.getSceneGeoCoding().getGeoPos(pixelPos, geoPos);
                centerSourceProduct.getSceneGeoCoding().getPixelPos(geoPos, pixelPos);
                Rectangle sourceRect = new Rectangle((int) pixelPos.x, (int) pixelPos.y, 90, 90);
                dataSource.readPixels(sourceRect, data);

                float valueFirstHalf = 0.0F;
                float valueSecondHalf = 0.0F;

                for (int i = 0; i < data.pixels.length; i++) {
                    int doy = data.pixels[i];
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, doy)) {
                        valueFirstHalf += data.areas[i];
                        baInLcFirstHalf.get(data.lcClasses[i])[targetPixelIndex] += data.areas[i];
                    } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, doy)) {
                        valueSecondHalf += data.areas[i];
                        baInLcSecondHalf.get(data.lcClasses[i])[targetPixelIndex] += data.areas[i];
                    }
                    areas[targetPixelIndex] += data.areas[i];
                }
                targetPixelIndex++;

                burnedAreaFirstHalf.setPixelFloat(x, y, valueFirstHalf);
                burnedAreaSecondHalf.setPixelFloat(x, y, valueSecondHalf);
                patchNumberFirstHalf.setPixelInt(x, y, data.patchCountFirstHalf);
                patchNumberSecondHalf.setPixelInt(x, y, data.patchCountSecondHalf);
            }
        }

        float[] baFirstHalf = baFirstHalfRasterData.getArray();
        float[] baSecondHalf = baSecondHalfRasterData.getArray();
        float[] errorsFirstHalf = predict(errorPredictor, baFirstHalf, areas);
        float[] errorsSecondHalf = predict(errorPredictor, baSecondHalf, areas);

        cleanErrors(baFirstHalf, errorsFirstHalf);
        cleanErrors(baSecondHalf, errorsSecondHalf);

        errorFirstHalf.setRasterData(new ProductData.Float(errorsFirstHalf));
        errorSecondHalf.setRasterData(new ProductData.Float(errorsSecondHalf));

        context.progress();

        // todo - debug output; remove this
//        File localFile = new File("./thomas-ba.nc");
//        ProductIO.writeProduct(targetProduct, localFile, "NetCDF4-BEAM", false);
//        Path path = new Path(String.format("hdfs://calvalus/calvalus/home/thomas/thomas-ba-%s.nc", tile));
//        FileSystem fs = path.getFileSystem(context.getConfiguration());
//        fs.delete(path, true);
//        FileUtil.copy(localFile, fs, path, false, context.getConfiguration());

        GridCell gridCell = new GridCell();
        gridCell.setBaFirstHalf(baFirstHalf);
        gridCell.setBaSecondHalf(baSecondHalf);
        gridCell.setPatchNumberFirstHalf(patchNumberFirstHalfData.getArray());
        gridCell.setPatchNumberSecondHalf(patchNumberSecondHalfData.getArray());
        gridCell.setErrorsFirstHalf(errorsFirstHalf);
        gridCell.setErrorsSecondHalf(errorsSecondHalf);
        gridCell.setBaInLcFirstHalf(baInLcFirstHalf);
        gridCell.setBaInLcSecondHalf(baInLcSecondHalf);

        context.write(new Text(String.format("%d-%02d-%s", year, month, tile)), gridCell);
        errorPredictor.dispose();
    }

    static Rectangle getSourceRect(PixelPos pixelPos) {
        final int sourcePixelCount = 90; // from 300m resolution to 0.25° -> each target pixel has 90 source pixels in x and y direction
        return new Rectangle((int) pixelPos.x - sourcePixelCount / 2, (int) pixelPos.y - sourcePixelCount / 2, sourcePixelCount, sourcePixelCount);
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, int pixel) {
        return pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6 && pixel != 999 && pixel != NO_DATA;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, int pixel) {
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth && pixel != 999 && pixel != NO_DATA;
    }

    private static void cleanErrors(float[] ba, float[] errors) {
        for (int i = 0; i < errors.length; i++) {
            if (ba[i] == 0.0) {
                errors[i] = 0.0F;
            }
        }
    }

    private float[] predict(ErrorPredictor errorPredictor, float[] ba, double[] areas) {
        try {
            return errorPredictor.predictError(ba, areas);
        } catch (ScriptException e) {
            throw new RuntimeException(String.format("Unable to predict error from BA %s, areas %s", Arrays.toString(ba), Arrays.toString(areas)), e);
        }
    }

    private Product createTargetProduct(String tile) throws IOException {
        Product target = new Product("target", "type", TARGET_RASTER_WIDTH, TARGET_RASTER_HEIGHT);
        try {
            target.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, TARGET_RASTER_WIDTH, TARGET_RASTER_HEIGHT, getEasting(tile), getNorthing(tile), 0.25, 0.25, 0.0, 0.0));
        } catch (FactoryException | TransformException e) {
            throw new IOException(e);
        }
        return target;
    }

    private float getEasting(String tile) {
        int hIndex = Integer.parseInt(tile.substring(4));
        return -180 + hIndex * 10;
    }

    private float getNorthing(String tile) {
        int vIndex = Integer.parseInt(tile.substring(1, 3));
        return 90 - vIndex * 10;
    }

    private static String getTile(Path path) {
        int startIndex = path.toString().indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
        return path.toString().substring(startIndex, startIndex + 6);
    }

    interface FireGridDataSource {

        void readPixels(Rectangle sourceRect, SourceData data) throws IOException;

        void setDoyFirstOfMonth(int doyFirstOfMonth);

        void setDoyLastOfMonth(int doyLastOfMonth);

        void setDoyFirstHalf(int doyFirstHalf);

        void setDoySecondHalf(int doySecondHalf);
    }

}
