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
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.mosaic.TileDataWritable;
import com.bc.calvalus.processing.mosaic.TileIndexWritable;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runs the Fire Formatting processor adapter.
 *
 * @author thomas
 */
public class FireFormattingMapper extends Mapper<NullWritable, NullWritable, TileIndexWritable, TileDataWritable> {

    private File cwd;
    private boolean debugScriptGenerator;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.cwd = new File(".");
        this.debugScriptGenerator = context.getConfiguration().getBoolean("calvalus.l2.debugScriptGenerator", false);
        InputSplit inputSplit = context.getInputSplit();
        KeywordHandler keywordHandler = process(ProgressMonitor.NULL, inputSplit, context);
        String[] outputFilesNames = keywordHandler.getOutputFiles();
        CalvalusLogger.getLogger().info("Writing output files: " + Arrays.toString(outputFilesNames));
        for (String outputFileName : outputFilesNames) {
            InputStream is = new BufferedInputStream(new FileInputStream(new File(cwd, outputFileName)));
            Path workPath = new Path(getWorkOutputDirectoryPath(context), outputFileName);
            OutputStream os = workPath.getFileSystem(context.getConfiguration()).create(workPath);
            ProductFormatter.copyAndClose(is, os, context);
        }
    }

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
        scriptGenerator.writeScriptFiles(cwd, debugScriptGenerator);

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
            int startIndex = path.toString().indexOf("BA_PIX_MER_") + "BA_PIX_MER_".length();
            String tile = path.toString().substring(startIndex, startIndex + 6);
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
