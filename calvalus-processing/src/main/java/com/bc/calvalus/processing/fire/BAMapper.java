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
import java.util.Arrays;

/**
 * Runs the BA processor adapter.
 *
 * @author thomas
 */
public class BAMapper extends Mapper<NullWritable, NullWritable, TileIndexWritable, TileDataWritable> {

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
        String tile = getTile(inputSplit);
        velocityContext.put("configuration", conf);
        String year = conf.get("calvalus.year");
        velocityContext.put("year", year);
        velocityContext.put("input_base_dir", conf.get("calvalus.input_base_dir"));
        velocityContext.put("firms_base_dir", conf.get("calvalus.firms_base_dir"));
        velocityContext.put("nocomb_base_dir", conf.get("calvalus.nocomb_base_dir"));
        velocityContext.put("tile", tile);

//        velocityContext.put("outputPath", configuration.getOgetOutputDirectoryPath());
//        velocityContext.put("workOutputPath", getWorkOutputDirectoryPath());

        scriptGenerator.addScriptResources(conf, "");
        if (!scriptGenerator.hasStepScript()) {
            throw new RuntimeException("No script for step 'process' available.");
        }
        scriptGenerator.writeScriptFiles(cwd, debugScriptGenerator);

        fetchInputFiles((CombineFileSplit) inputSplit, conf, tile);

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

    private static void fetchInputFiles(CombineFileSplit inputSplit, Configuration conf, String tile) throws IOException {
        for (Path path : inputSplit.getPaths()) {
            int startIndex = path.toString().indexOf("/" + tile + "/") + 1;
            File targetFile = new File(path.toString().substring(startIndex));
            targetFile.getParentFile().mkdirs();
            CalvalusProductIO.copyFileToLocal(path, targetFile, conf);
        }
    }

    private String getTile(InputSplit inputSplit) {
        CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;
        String name = combineFileSplit.getPath(0).getName();    // ok, if there are no paths, there is no input split as well.
        return name.substring(name.length() - "vXXhXX.nc".length(), name.length() - ".nc".length());
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
