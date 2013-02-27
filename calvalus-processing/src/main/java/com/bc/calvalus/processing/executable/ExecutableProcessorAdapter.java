/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.executable;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.resource.ReaderResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.datamodel.ProductData;

import java.awt.Rectangle;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Collection;

/**
 * A processor adapter that uses an executable to process an input product.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorAdapter extends ProcessorAdapter {

    private final File cwd;
    private String[] outputFilesNames;
    private String inputFileName;
    private boolean skipProcessing = false;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        super(mapContext);
        this.cwd = new File(".");
    }

    @Override
    public void prepareProcessing() throws IOException {
        Configuration conf = getConfiguration();
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PREPARE, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        Path inputPath = getInputPath();
        Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
        velocityContext.put("inputPath", inputPath);
        velocityContext.put("outputPath", outputPath);

        addScriptResources(conf, scriptGenerator);
        if (scriptGenerator.hasStepScript()) {
            scriptGenerator.writeScriptFiles(cwd);

            String[] cmdArray = {"./prepare", inputPath.toString(), outputPath.toString()};
            Process process = Runtime.getRuntime().exec(cmdArray);
            String processLogName = bundle + "-" + executable + "-prepare";
            KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

            new ProcessObserver(process).
                    setName(processLogName).
                    setHandler(keywordHandler).
                    start();
            inputFileName = keywordHandler.getInputFile();
            skipProcessing = keywordHandler.skipProcessing();
        }
    }

    @Override
    public boolean canSkipInputProduct() throws IOException {
        return skipProcessing;
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("Exec Level 2");
        Configuration conf = getConfiguration();
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS, "");

        Rectangle inputRectangle = getInputRectangle();
        File inputFile;
        if (inputFileName != null) {
            inputFile = new File(inputFileName);
        } else {
            inputFile = copyProductToLocal(getInputPath());
        }

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        velocityContext.put("inputFile", inputFile);
        velocityContext.put("inputRectangle", inputRectangle);
        velocityContext.put("outputPath", FileOutputFormat.getOutputPath(getMapContext()));

        addScriptResources(conf, scriptGenerator);
        if (!scriptGenerator.hasStepScript()) {
            throw new RuntimeException("No script for step 'process' available.");
        }
        scriptGenerator.writeScriptFiles(cwd);

        String[] cmdArray = {"./process", inputFile.getCanonicalPath()};
        Process process = Runtime.getRuntime().exec(cmdArray);
        String processLogName = bundle + "-" + executable + "-process";
        KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

        new ProcessObserver(process).
                setName(processLogName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();

        outputFilesNames = keywordHandler.getOutputFiles();
        return outputFilesNames.length;
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            Product product = ProductIO.readProduct(new File(cwd, outputFilesNames[0]));
            getLogger().info(String.format("Opened product width = %d height = %d",
                                           product.getSceneRasterWidth(),
                                           product.getSceneRasterHeight()));
            ProductReader productReader = product.getProductReader();
            if (productReader != null) {
                getLogger().info(String.format("ReaderPlugin: %s", productReader.toString()));
            }
            if (product.getStartTime() == null || product.getEndTime() == null) {
                setSceneRasterStartAndStopTime(getInputProduct(), product, getInputRectangle());
            }
            return product;
        }
        return null;
    }

    private void setSceneRasterStartAndStopTime(Product sourceProduct, Product targetProduct, Rectangle inputRectangle) {
        final ProductData.UTC startTime = sourceProduct.getStartTime();
        final ProductData.UTC stopTime = sourceProduct.getEndTime();
        if (startTime != null && stopTime != null && inputRectangle != null) {
            final double height = sourceProduct.getSceneRasterHeight();
            final double regionY = inputRectangle.getY();
            final double regionHeight = inputRectangle.getHeight();
            final double dStart = startTime.getMJD();
            final double dStop = stopTime.getMJD();
            final double vPerLine = (dStop - dStart) / (height - 1);
            final double newStart = vPerLine * regionY + dStart;
            final double newStop = vPerLine * (regionHeight - 1) + newStart;
            targetProduct.setStartTime(new ProductData.UTC(newStart));
            targetProduct.setEndTime(new ProductData.UTC(newStop));
        } else {
            targetProduct.setStartTime(startTime);
            targetProduct.setEndTime(stopTime);
        }
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws Exception {
        if (outputFilesNames != null && outputFilesNames.length > 0) {

            Configuration conf = getConfiguration();
            String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE);
            String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
            String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

            ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.FINALIZE, executable);
            VelocityContext velocityContext = scriptGenerator.getVelocityContext();
            velocityContext.put("system", System.getProperties());
            velocityContext.put("configuration", conf);
            velocityContext.put("parameterText", processorParameters);
            velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

            velocityContext.put("outputFileNames", outputFilesNames);
            Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
            velocityContext.put("outputPath", outputPath);

            addScriptResources(conf, scriptGenerator);
            if (scriptGenerator.hasStepScript()) {
                scriptGenerator.writeScriptFiles(cwd);

                String[] cmdArray = new String[outputFilesNames.length + 2];
                cmdArray[0] = "./finalize";
                System.arraycopy(outputFilesNames, 0, cmdArray, 1, outputFilesNames.length);
                cmdArray[cmdArray.length-1] = outputPath.toString();

                Process process = Runtime.getRuntime().exec(cmdArray);
                String processLogName = bundle + "-" + executable + "-finalize";
                KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

                new ProcessObserver(process).
                        setName(processLogName).
                        setProgressMonitor(pm).
                        setHandler(keywordHandler).
                        start();
            } else {
                MapContext mapContext = getMapContext();
                for (String outputFileName : outputFilesNames) {
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(new File(cwd, outputFileName)));
                    OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFileName);
                    ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
                }
            }
        }
    }

    @Override
    public Path getOutputPath() throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            try {
                Path workOutputPath = FileOutputFormat.getWorkOutputPath(getMapContext());
                return new Path(workOutputPath, outputFilesNames[0]);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        return null;
    }

    @Override
    public boolean supportsPullProcessing() {
        return false;
    }

    private void addScriptResources(Configuration conf, ScriptGenerator scriptGenerator) throws IOException {
        Collection<String> scriptFiles = conf.getStringCollection("calvalus.l2.scriptFiles");
        FileSystem fs = FileSystem.get(conf);
        for (String scriptFile : scriptFiles) {
            Path scriptFilePath = new Path(scriptFile);
            InputStream inputStream = fs.open(scriptFilePath);
            Reader reader = new InputStreamReader(inputStream);
            scriptGenerator.addResource(new ReaderResource(scriptFile, reader));
        }
    }
}
