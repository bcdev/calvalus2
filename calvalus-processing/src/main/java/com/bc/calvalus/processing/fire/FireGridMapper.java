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
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.executable.KeywordHandler;
import com.bc.calvalus.processing.executable.ScriptGenerator;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;
import org.renjin.sexp.DoubleArrayVector;
import org.renjin.sexp.DoubleVector;
import org.renjin.sexp.SEXP;
import org.renjin.sexp.Vector;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.awt.Rectangle;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Runs the fire formatting grid mapper.
 *
 * @author thomas
 * @author marcop
 */
public class FireGridMapper extends Mapper<LongWritable, FileSplit, IntWritable, GridCell> {

    public static final int TARGET_RASTER_WIDTH = 40;
    public static final int TARGET_RASTER_HEIGHT = 40;
    public static final int NO_DATA = -1;

    private static final float ONE_PIXEL_AREA = 300 * 300;
    private static final Logger LOG = CalvalusLogger.getLogger();

    private static ScriptEngine engine;

    @Override
    public void run(Context context) throws IOException, InterruptedException {

//        ScriptEngineManager manager = new ScriptEngineManager();
        // create a Renjin engine:
//        engine = manager.getEngineByName("Renjin");
//        if (engine == null) {
//            throw new IllegalStateException();
//        }

//        try {
//            testMethod1();
//            testPrediction();
//        } catch (ScriptException | URISyntaxException e) {
//            e.printStackTrace();
//        }

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

        Map<Position, Product> neighbourProducts = new HashMap<>();
        for (int i = 1; i < paths.length; i++) {
            Path path = paths[i];
            File localFile = CalvalusProductIO.copyFileToLocal(path, context.getConfiguration());
            neighbourProducts.put(findPosition(path.getName(), tile), ProductIO.readProduct(localFile));
        }

        Product target = createTargetProduct(tile);

        FireGridDataSource dataSource = new FireGridDataSourceImpl(centerSourceProduct, neighbourProducts);

        Band burnedAreaFirstHalf = target.addBand("burned_area_first_half", ProductData.TYPE_FLOAT32);
        Band burnedAreaSecondHalf = target.addBand("burned_area_second_half", ProductData.TYPE_FLOAT32);
        PixelPos pixelPos = new PixelPos();
        GeoPos geoPos = new GeoPos();
        int[] pixels = new int[90 * 90];
        burnedAreaFirstHalf.setRasterData(new ProductData.Float(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT));
        burnedAreaSecondHalf.setRasterData(new ProductData.Float(TARGET_RASTER_WIDTH * TARGET_RASTER_HEIGHT));
        for (int y = 0; y < TARGET_RASTER_HEIGHT; y++) {
            LOG.info(String.format("Processing line %d of target raster.", y));
            for (int x = 0; x < TARGET_RASTER_WIDTH; x++) {
                pixelPos.x = x;
                pixelPos.y = y;
                target.getSceneGeoCoding().getGeoPos(pixelPos, geoPos);
                centerSourceProduct.getSceneGeoCoding().getPixelPos(geoPos, pixelPos);
                Rectangle sourceRect = getSourceRect(pixelPos);
                LOG.info("sourceRect=" + sourceRect.toString());
                Arrays.fill(pixels, NO_DATA);
                dataSource.readPixels(sourceRect, pixels);

                float valueFirstHalf = 0.0F;
                float valueSecondHalf = 0.0F;

                for (int pixelFloat : pixels) {
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, pixelFloat)) {
                        valueFirstHalf += ONE_PIXEL_AREA;
                    } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, pixelFloat)) {
                        valueSecondHalf += ONE_PIXEL_AREA;
                    }
                }
                burnedAreaFirstHalf.setPixelFloat(x, y, valueFirstHalf);
                burnedAreaSecondHalf.setPixelFloat(x, y, valueSecondHalf);
            }
        }

        ProductIO.writeProduct(target, "/tmp/deleteme/thomas-ba.nc", "NetCDF4-BEAM");
    }

    static Position findPosition(String filename, String centerTile) {
        final int[] tileIndices = FireGridInputFormat.getTileIndices(filename);
        int centerX = Integer.parseInt(centerTile.substring(4, 6));
        int centerY = Integer.parseInt(centerTile.substring(1, 3));
        if (centerX > tileIndices[1]) {
            // left
            if (centerY > tileIndices[0]) {
                return Position.TOP_LEFT;
            } else if (centerY == tileIndices[0]) {
                return Position.CENTER_LEFT;
            } else if (centerY < tileIndices[0]) {
                return Position.BOTTOM_LEFT;
            }
        } else if (centerX == tileIndices[1]) {
            // center
            if (centerY > tileIndices[0]) {
                return Position.TOP_CENTER;
            } else if (centerY == tileIndices[0]) {
                return Position.CENTER;
            } else if (centerY < tileIndices[0]) {
                return Position.BOTTOM_CENTER;
            }
        } else if (centerX < tileIndices[1]) {
            // right
            if (centerY > tileIndices[0]) {
                return Position.TOP_RIGHT;
            } else if (centerY == tileIndices[0]) {
                return Position.CENTER_RIGHT;
            } else if (centerY < tileIndices[0]) {
                return Position.BOTTOM_RIGHT;
            }
        }
        return null;
    }

    private Product createTargetProduct(String tile) throws IOException {
        Product target = new Product("target", "type", TARGET_RASTER_WIDTH, TARGET_RASTER_HEIGHT);
        try {
            target.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, TARGET_RASTER_WIDTH, TARGET_RASTER_HEIGHT, getEasting(tile), getNorthing(tile), 0.25, 0.25));
        } catch (FactoryException | TransformException e) {
            throw new IOException(e);
        }
        return target;
    }

    static Rectangle getSourceRect(PixelPos pixelPos) {
        final int sourcePixelCount = 90; // from 300m resolution to 0.25° -> each target pixel has 90 source pixels in x and y direction -> 8100 pixels
        return new Rectangle((int) pixelPos.x - sourcePixelCount / 2, (int) pixelPos.y - sourcePixelCount / 2, sourcePixelCount, sourcePixelCount);
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, int pixel) {
        return pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6 && pixel != 999 && pixel != NO_DATA;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, int pixel) {
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth && pixel != 999 && pixel != NO_DATA;
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

//    @Override
//    public void run(Context context) throws IOException, InterruptedException {
//        this.cwd = new File(".");
//        this.debugScriptGenerator = context.getConfiguration().getBoolean("calvalus.l2.debugScriptGenerator", false);
//        InputSplit inputSplit = context.getInputSplit();
//        KeywordHandler keywordHandler = process(ProgressMonitor.NULL, inputSplit, context);
//        String[] outputFilesNames = keywordHandler.getOutputFiles();
//        CalvalusLogger.getLogger().info("Writing output files: " + Arrays.toString(outputFilesNames));
//        for (String outputFileName : outputFilesNames) {
//            InputStream is = new BufferedInputStream(new FileInputStream(new File(cwd, outputFileName)));
//            Path workPath = new Path(getWorkOutputDirectoryPath(context), outputFileName);
//            OutputStream os = workPath.getFileSystem(context.getConfiguration()).create(workPath);
//            ProductFormatter.copyAndClose(is, os, context);
//        }
//    }

    private KeywordHandler process(ProgressMonitor pm, InputSplit inputSplit, Context context) throws IOException, InterruptedException {
        pm.setSubTaskName("Exec Level 2");
        Configuration conf = context.getConfiguration();
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("configuration", conf);
        String year = conf.get("calvalus.year");
        String month = conf.get("calvalus.month");
        velocityContext.put("year", year);
        velocityContext.put("month", month);

        scriptGenerator.addScriptResources(conf, "");
        if (!scriptGenerator.hasStepScript()) {
            throw new RuntimeException("No script for step 'process' available.");
        }
//        scriptGenerator.writeScriptFiles(cwd, false);

        fetchInputFiles((CombineFileSplit) inputSplit, conf);
        fetchAuxdataFiles((CombineFileSplit) inputSplit, conf);

        String[] cmdArray = {"./process"};
        Process process = Runtime.getRuntime().exec(cmdArray);
        String processLogName = executable + "-process";
        KeywordHandler keywordHandler = new KeywordHandler(processLogName, context);

        new ProcessObserver(process).
                setName(processLogName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();
        return keywordHandler;
    }

    private void fetchAuxdataFiles(CombineFileSplit inputSplit, Configuration conf) throws IOException {
        // identifies the necessary aux data for the current split, and copies it over
//        File landCoverDir = new File(this.cwd, "landcover");
//        landCoverDir.mkdirs();
        List<String> tileList = new ArrayList<>();
        for (Path path : inputSplit.getPaths()) {
            // path = hdfs://calvalus/calvalus/projects/fire/meris-ba/2008/BA_PIX_MER_v02h25_200806_v3.0.tif
            String tile = getTile(path);
            if (tileList.contains(tile)) {
                continue;
            }
            String tileXindex = tile.substring(1, 3);
            String tileYindex = tile.substring(4, 6);
            String[] fileNameStubs = {
                    "GlobCover_L%sC%s.tif",
                    "GlobCover_v%sh%s",
                    "GlobCover_v%sh%s.aux.xml",
                    "GlobCover_v%sh%s.hdr",
                    "GlobCover_v%sh%s_hr",
                    "GlobCover_v%sh%s_hr.hdr"
            };
            for (String stub : fileNameStubs) {
                Path auxDataFile = new Path(String.format("hdfs://calvalus/calvalus/auxiliary/fire/formatting/" + stub, tileXindex, tileYindex));
                if (auxDataFile.getFileSystem(conf).exists(auxDataFile)) {
//                    File localFile = new File(landCoverDir, String.format(stub, tileXindex, tileYindex));
//                    CalvalusProductIO.copyFileToLocal(auxDataFile, localFile, conf);
                }
            }
            tileList.add(tile);
        }
    }

    private static void fetchInputFiles(CombineFileSplit inputSplit, Configuration conf) throws IOException {
        for (Path path : inputSplit.getPaths()) {
            CalvalusProductIO.copyFileToLocal(path, conf);
        }
    }

    private Path getWorkOutputDirectoryPath(Context context) throws IOException {
        try {
            Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            return appendDatePart(workOutputPath, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    private Path appendDatePart(Path path, Context context) {
        String year = context.getConfiguration().get("calvalus.year");
        return new Path(path, year);
    }

    public enum Position {
        TOP_LEFT,
        CENTER_LEFT,
        BOTTOM_LEFT,
        TOP_CENTER,
        CENTER,
        BOTTOM_CENTER,
        TOP_RIGHT,
        CENTER_RIGHT,
        BOTTOM_RIGHT,
    }

    public interface FireGridDataSource {

        void readPixels(Rectangle sourceRect, int[] pixels) throws IOException;

    }


    public void testMethod1() throws ScriptException {

        engine.eval("df <- data.frame(x=1:10, y=(1:10)+rnorm(n=10))");
        engine.eval("print(df)");
        engine.eval("print(lm(y ~ x, df))");


        engine.eval("rVector=c(1,2,3,4,5)");
        engine.eval("meanVal=mean(rVector)");
        SEXP meanVal = (SEXP) engine.eval("meanVal");
        double mean = meanVal.asReal();

        LOG.info("mean=" + mean);

        Vector x = (Vector) engine.eval("x <- c(6, 7, 8, 9)");
        LOG.info("x=" + x);
        engine.put("y", new double[]{1d, 2d, 3d, 4d});
        engine.put("z", new DoubleArrayVector(1, 2, 3, 4, 5));

        LOG.info(String.valueOf(((SEXP) engine.eval("out <- sum(y)")).asReal()));
        LOG.info(String.valueOf(((SEXP) engine.eval("out <- sum(z)")).asReal()));
    }

    public void testPrediction() throws URISyntaxException, ScriptException {
        File resFile = new File(getClass().getResource("codiR_cecr_GridProd.RData").toURI());
        String absolutePath = resFile.getAbsolutePath().replace("\\", "\\\\");
        engine.eval("load(file = \"" + absolutePath + "\")");
        engine.put("bap", new double[]{1, 2, 3, 4, 5, 1, 2, 3, 4, 5});
        engine.eval("nwd = data.frame(gpg=bap)");
        DoubleVector pred = (DoubleVector) engine.eval("predict(ug, nwd)");
        double[] predResult = pred.toDoubleArray();
        LOG.info(Arrays.toString(predResult));
    }

}
