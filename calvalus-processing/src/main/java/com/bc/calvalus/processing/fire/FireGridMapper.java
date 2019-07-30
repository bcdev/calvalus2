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

import java.io.File;
import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Runs the fire formatting grid mapper.
 *
 * @author thomas
 */
public class FireGridMapper extends Mapper<LongWritable, FileSplit, IntWritable, GridCell> {

    private static final float ONE_PIXEL_AREA = 300 * 300;
    private File cwd;
    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        int year = Integer.parseInt(context.getConfiguration().get("calvalus.year"));
        int month = Integer.parseInt(context.getConfiguration().get("calvalus.month"));

        int doyFirstOfMonth = Year.of(year).atMonth(month).atDay(1).getDayOfYear();
        int doyLastOfMonth = Year.of(year).atMonth(month).atDay(Year.of(year).atMonth(month).lengthOfMonth()).getDayOfYear();

        int doyFirstHalf = Year.of(year).atMonth(month).atDay(7).getDayOfYear();
        int doySecondHalf = Year.of(year).atMonth(month).atDay(22).getDayOfYear();

        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        Path path = inputSplit.getPath();
        LOG.info("inputSplitPath = " + path);
        File localFile = CalvalusProductIO.copyFileToLocal(path, context.getConfiguration());
        Product product = ProductIO.readProduct(localFile);

        String tile = getTile(path);

        Product target = new Product("target", "type", 40, 40);
        try {
            target.setSceneGeoCoding(new CrsGeoCoding(DefaultGeographicCRS.WGS84, 40, 40, getEasting(tile), getNorthing(tile), 0.25, 0.25));
        } catch (FactoryException | TransformException e) {
            throw new IOException(e);
        }
        Band band_1 = product.getBand("band_1");
        Band burnedAreaFirstHalf = target.addBand("burned_area_first_half", ProductData.TYPE_FLOAT32);
        Band burnedAreaSecondHalf = target.addBand("burned_area_second_half", ProductData.TYPE_FLOAT32);
        PixelPos pixelPos = new PixelPos();
        GeoPos geoPos = new GeoPos();
        for (int y = 0; y < 40; y++) {
            for (int x = 0; x < 40; x++) {
                pixelPos.x = x;
                pixelPos.y = y;
                target.getSceneGeoCoding().getGeoPos(pixelPos, geoPos);
                product.getSceneGeoCoding().getPixelPos(geoPos, pixelPos);
                float valueFirstHalf = 0.0F;
                float valueSecondHalf = 0.0F;
                float[] pixels = new float[90 * 90];
                band_1.readPixels((int) (pixelPos.x - 45), (int) (pixelPos.y - 45), 90, 90, pixels);
                for (int i = 0; i < 90 * 90; i++) {
                    float pixelFloat = pixels[i];
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

        // TODO: check whether this is still required
        ProductIO.writeProduct(target, "/tmp/deleteme/thomas-ba.nc", "NetCDF4-BEAM");
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, float pixelFloat) {
        return pixelFloat >= doyFirstOfMonth && pixelFloat < doySecondHalf - 6 && pixelFloat != 999.0;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, float pixelFloat) {
        return pixelFloat > doyFirstHalf + 8 && pixelFloat <= doyLastOfMonth && pixelFloat != 999.0;
    }

    private float getEasting(String tile) {
        int hIndex = Integer.parseInt(tile.substring(4));
        return hIndex * 10;
    }

    private float getNorthing(String tile) {
        int vIndex = Integer.parseInt(tile.substring(1, 3));
        return vIndex * 10;
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
        scriptGenerator.writeScriptFiles(cwd, false);

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
        File landCoverDir = new File(this.cwd, "landcover");
        landCoverDir.mkdirs();
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
                    File localFile = new File(landCoverDir, String.format(stub, tileXindex, tileYindex));
                    CalvalusProductIO.copyFileToLocal(auxDataFile, localFile, conf);
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

}
