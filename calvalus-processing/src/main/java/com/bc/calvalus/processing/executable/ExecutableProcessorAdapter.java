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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductReader;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.logging.Level;

/**
 * A processor adapter that uses an executable to process an input product.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorAdapter extends ProcessorAdapter {

    private final File cwd;
    private final String parameterSuffix;
    private String[] outputFilesNames;
    private boolean skipProcessing = false;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        this(mapContext, "");
    }

    public ExecutableProcessorAdapter(MapContext mapContext, String parameterSuffix) {
        super(mapContext);
        this.parameterSuffix = parameterSuffix;
        this.cwd = new File(".");
    }

    @Override
    public void prepareProcessing() throws IOException {
        Configuration conf = getConfiguration();
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE+ parameterSuffix);
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix);

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

        scriptGenerator.addScriptResources(conf, parameterSuffix);
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
            setInputFile(new File(keywordHandler.getInputFile()));
            skipProcessing = keywordHandler.skipProcessing();
        }
    }

    @Override
    public boolean canSkipInputProduct() throws IOException {
        return skipProcessing;
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {

        Path inputPath = getInputPath();
        File inputFile = getInputFile();
        if (inputFile == null) {
            inputFile = copyFileToLocal(inputPath);
            setInputFile(inputFile);
        }
        Rectangle productRect = null;
        Rectangle inputRectangle = getInputRectangle();
        if (inputRectangle != null) {
            Product inputProduct = getInputProduct();
            productRect = new Rectangle(0, 0, inputProduct.getSceneRasterWidth(), inputProduct.getSceneRasterHeight());
        }

        outputFilesNames = processInput(pm, inputRectangle, inputPath, inputFile, productRect, null);
        return outputFilesNames.length;
    }

    public File getCurrentWorkingDir() {
        return cwd;
    }

    public String[] processInput(ProgressMonitor pm, Rectangle inputRectangle, Path inputPath, File inputFile, Rectangle productRectangle, Map<String, String> velocityProps) throws IOException {
        KeywordHandler keywordHandler = process(pm, inputRectangle, inputPath, inputFile, productRectangle, velocityProps);

        return keywordHandler.getOutputFiles();
    }

    public KeywordHandler process(ProgressMonitor pm, Rectangle inputRectangle, Path inputPath, File inputFile, Rectangle productRectangle, Map<String, String> velocityProps) throws IOException {
        pm.setSubTaskName("Exec Level 2");
        Configuration conf = getConfiguration();
        String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE + parameterSuffix);
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix, "");

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PROCESS, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        velocityContext.put("inputPath", inputPath);
        velocityContext.put("inputFile", inputFile);
        velocityContext.put("inputRectangle", inputRectangle);

        velocityContext.put("productRectangle", productRectangle);

        velocityContext.put("outputPath", FileOutputFormat.getOutputPath(getMapContext()));

        if (velocityProps != null) {
            for (Map.Entry<String, String> entry : velocityProps.entrySet()) {
                velocityContext.put(entry.getKey(), entry.getValue());
            }
        }

        scriptGenerator.addScriptResources(conf, parameterSuffix);
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
        return keywordHandler;
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
            if (hasInvalidStartAndStopTime(product)) {
                getLogger().log(Level.INFO, "Processed Product has no or invalid start/stop time. Copying from input.");
                // When processing with Polymere no time information is attached to the product.
                // When processing with MEGS and input rectangle the start time is invalid.
                // Therefor we have to adjust it here.
                copySceneRasterStartAndStopTime(getInputProduct(), product, getInputRectangle());
            }
            return product;
        }
        return null;
    }


    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {

            Configuration conf = getConfiguration();
            String bundle = conf.get(JobConfigNames.CALVALUS_L2_BUNDLE + parameterSuffix);
            String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
            String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix);

            ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.FINALIZE, executable);
            VelocityContext velocityContext = scriptGenerator.getVelocityContext();
            velocityContext.put("system", System.getProperties());
            velocityContext.put("configuration", conf);
            velocityContext.put("parameterText", processorParameters);
            velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

            velocityContext.put("outputFileNames", outputFilesNames);
            Path outputPath = FileOutputFormat.getOutputPath(getMapContext());
            velocityContext.put("outputPath", outputPath);

            scriptGenerator.addScriptResources(conf, parameterSuffix);
            if (scriptGenerator.hasStepScript()) {
                scriptGenerator.writeScriptFiles(cwd);

                String[] cmdArray = new String[outputFilesNames.length + 2];
                cmdArray[0] = "./finalize";
                System.arraycopy(outputFilesNames, 0, cmdArray, 1, outputFilesNames.length);
                cmdArray[cmdArray.length - 1] = outputPath.toString();

                Process process = Runtime.getRuntime().exec(cmdArray);
                String processLogName = bundle + "-" + executable + "-finalize";
                KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

                new ProcessObserver(process).
                        setName(processLogName).
                        setProgressMonitor(pm).
                        setHandler(keywordHandler).
                        start();
            } else {
                pm.beginTask("saving", 1);
                MapContext mapContext = getMapContext();
                for (String outputFileName : outputFilesNames) {
                    InputStream inputStream = new BufferedInputStream(
                            new FileInputStream(new File(cwd, outputFileName)));
                    OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFileName);
                    ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
                }
                pm.done();
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

}
