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
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.beam.LandsatCalvalusReaderPlugin;
import com.bc.calvalus.processing.beam.PathConfiguration;
import com.bc.calvalus.processing.beam.Sentinel2CalvalusReaderPlugin;
import com.bc.calvalus.processing.beam.SnapGraphAdapter;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.utils.ProductTransformation;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.runtime.internal.DirScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.velocity.VelocityContext;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
    private final boolean debugScriptGenerator;
    private String[] outputFilesNames;
    private boolean skipProcessing = false;
    private List<String> additionalInputFiles = null;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        this(mapContext, "");
    }

    public ExecutableProcessorAdapter(MapContext mapContext, String parameterSuffix) {
        super(mapContext);
        this.parameterSuffix = parameterSuffix;
        this.cwd = new File(".");
        this.debugScriptGenerator = mapContext.getConfiguration().getBoolean("calvalus.l2.debugScriptGenerator", false);
    }

    @Override
    public void prepareProcessing() throws IOException {
        super.prepareProcessing();
        Configuration conf = getConfiguration();
        String user = conf.get("mapreduce.job.user.name");
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix);
        if (getInputParameters().length > 0) {
            final StringBuilder accu = processorParameters != null ? new StringBuilder(processorParameters) : new StringBuilder();
            for (int i=0; i<getInputParameters().length; i+=2) {
//                if ("output".equals(getInputParameters()[i])) {
//                    // skip, will be considered in finalise
//                } else
                if ("regionGeometry".equals(getInputParameters()[i])) {
                    conf.set("calvalus.regionGeometry", getInputParameters()[i + 1]);
                } else {
                    if (accu.length() > 0) {
                        accu.append('\n');
                    }
                    accu.append(getInputParameters()[i]);
                    accu.append('=');
                    accu.append(getInputParameters()[i + 1]);
                }
            }
            processorParameters = accu.toString();
        }

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.PREPARE, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        Path inputPath = getInputPath();
        Path outputPath = getOutputDirectoryPath();
        velocityContext.put("inputPath", inputPath);
        velocityContext.put("outputPath", outputPath);
        velocityContext.put("workOutputPath", getWorkOutputDirectoryPath());
        velocityContext.put("GlobalFunctions", new SnapGraphAdapter.GlobalFunctions(getLogger()));

        scriptGenerator.addScriptResources(conf, parameterSuffix);
        if (scriptGenerator.hasStepScript()) {
            scriptGenerator.writeScriptFiles(cwd, debugScriptGenerator);
            
            getLogger().info("prepare: " + executable + " " + inputPath.toString()+ " " + outputPath.toString());
            String[] cmdArray = {"./prepare", inputPath.toString(), outputPath.toString()};
            String[] env = new String[] { "HADOOP_USER_NAME=" + user };
            Process process = Runtime.getRuntime().exec(cmdArray, env);
            String processLogName = executable + "-prepare";
            KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

            new ProcessObserver(process).
                    setName(processLogName).
                    setHandler(keywordHandler).
                    start();
            skipProcessing = keywordHandler.skipProcessing();
            String inputFile = keywordHandler.getInputFile();
            if (inputFile != null) {
                setInputFile(new File(inputFile));
            }
        }
    }

    @Override
    public boolean canSkipInputProduct() throws IOException {
        return skipProcessing;
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {

        Path inputPath = getInputPath();
        Path[] additionalInputPaths = getAdditionalInputPaths();
        File inputFile = getInputFile();
        if (inputFile == null) {
            inputFile = CalvalusProductIO.copyFileToLocal(inputPath, getConfiguration());
            setInputFile(inputFile);
        }
        if (additionalInputPaths != null) {
            additionalInputFiles = new ArrayList<String>();
            for (Path additionalInputPath : additionalInputPaths) {
                if (additionalInputPath.getFileSystem(getConfiguration()).exists(additionalInputPath)) {
                    additionalInputFiles.add(CalvalusProductIO.copyFileToLocal(additionalInputPath, getConfiguration()).getName());
                }
            }
        }
        if (getMapContext().getInputSplit() instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) getMapContext().getInputSplit();
            getMapContext().getCounter("Direct File System Counters", "FILE_SPLIT_BYTES_READ").setValue(fileSplit.getLength());
        }
        Rectangle productRect = null;
        Rectangle inputRectangle = getInputRectangle();
        if (inputRectangle != null) {
            Product inputProduct = getInputProduct();
            if (inputProduct != null) {
                productRect = new Rectangle(inputProduct.getSceneRasterWidth(), inputProduct.getSceneRasterHeight());
            } else {
                productRect = inputRectangle;
            }
        }

        outputFilesNames = processInput(pm, inputRectangle, inputPath, inputFile, productRect, null);
        return outputFilesNames.length > 0;
    }

    public File getCurrentWorkingDir() {
        return cwd;
    }

    public String[] processInput(ProgressMonitor pm, Rectangle inputRectangle, Path inputPath, File inputFile, Rectangle productRectangle, Map<String, String> velocityProps) throws IOException {
        KeywordHandler keywordHandler = process(pm, inputRectangle, inputPath, inputFile, productRectangle, velocityProps);

        String productTransformation = keywordHandler.getProductTransformation();
        if (productTransformation != null) {
            System.out.println("productTransformation = " + productTransformation);
            if (productRectangle == null) {
                Product inputProduct = getInputProduct();
                productRectangle = new Rectangle(inputProduct.getSceneRasterWidth(), inputProduct.getSceneRasterHeight());
            }
            ProductTransformation pTrans = ProductTransformation.parse(productTransformation, productRectangle);
            AffineTransform transform = pTrans.getTransform();
            System.out.println("transform = " + transform);
            setInput2OutputTransform(transform);
        }

        return keywordHandler.getOutputFiles();
    }

    public KeywordHandler process(ProgressMonitor pm, Rectangle inputRectangle, Path inputPath, File inputFile, Rectangle productRectangle, Map<String, String> velocityProps) throws IOException {
        pm.setSubTaskName("Exec Level 2");
        Configuration conf = getConfiguration();
        String user = conf.get("mapreduce.job.user.name");
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix, "");
        if (getInputParameters().length > 0) {
            final StringBuilder accu = new StringBuilder(processorParameters);
            for (int i=0; i<getInputParameters().length; i+=2) {
                if ("input".equals(getInputParameters()[i])) {
                    // skip, has been considered as additional input
//                } else if ("output".equals(getInputParameters()[i])) {
//                    // skip, will be considered in finalise
                } else if ("regionGeometry".equals(getInputParameters()[i])) {
                    conf.set("calvalus.regionGeometry", getInputParameters()[i + 1]);
                } else {
                    if (accu.length() > 0) {
                        accu.append('\n');
                    }
                    accu.append(getInputParameters()[i]);
                    accu.append('=');
                    accu.append(getInputParameters()[i + 1]);
                }
            }
            processorParameters = accu.toString();
        }

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
        velocityContext.put("outputPath", getOutputDirectoryPath());
        velocityContext.put("workOutputPath", getWorkOutputDirectoryPath());
        velocityContext.put("GlobalFunctions", new SnapGraphAdapter.GlobalFunctions(getLogger()));

        if (velocityProps != null) {
            for (Map.Entry<String, String> entry : velocityProps.entrySet()) {
                velocityContext.put(entry.getKey(), entry.getValue());
            }
        }

        scriptGenerator.addScriptResources(conf, parameterSuffix);
        if (!scriptGenerator.hasStepScript()) {
            throw new RuntimeException("No script for step 'process' available.");
        }
        scriptGenerator.writeScriptFiles(cwd, debugScriptGenerator);

        getLogger().info("process: " + executable + " " + inputFile.getCanonicalPath());
        String[] cmdArray;
        if (additionalInputFiles == null) {
            cmdArray = new String[]{"./process", inputFile.getCanonicalPath()};
        } else {
            cmdArray = new String[2 + additionalInputFiles.size()];
            cmdArray[0] = "./process";
            cmdArray[1] = inputFile.getCanonicalPath();
            System.arraycopy(additionalInputFiles.toArray(new String[0]), 0, cmdArray, 2, additionalInputFiles.size());
        }
        String[] env = new String[] { "HADOOP_USER_NAME=" + user };
        Process process = Runtime.getRuntime().exec(cmdArray, env);
        String processLogName = executable + "-process";
        KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

        new ProcessObserver(process).
                setName(processLogName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();
        return keywordHandler;
    }

    private boolean isSentinel2(String filename) {
        return filename.matches("^S2.*_MSIL1C.*zip") ||
                    filename.matches("^S2.*_MSIL2A.*zip");
    }

    private boolean isLandsat(String filename) {
        for (String pattern : LandsatCalvalusReaderPlugin.FILENAME_PATTERNS) {
            if (filename.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            Product product = ProductIO.readProduct(new File(cwd, outputFilesNames[0]));
            CalvalusProductIO.printProductOnStdout(product, "executable output");
            File productFileLocation = product.getFileLocation();
            if (isSentinel2(outputFilesNames[0])) {
                Map<String, Object> params = new HashMap<>();
                params.put("referenceBand", "B5");
                product = GPF.createProduct("Resample", params, product);
                CalvalusProductIO.printProductOnStdout(product, "resampled");
                product.setFileLocation(productFileLocation);
            } else if (isLandsat(outputFilesNames[0])) {
                Map<String, Object> params = new HashMap<>();
                params.put("referenceBand", "red");
                product = GPF.createProduct("Resample", params, product);
                CalvalusProductIO.printProductOnStdout(product, "resampled");
                product.setFileLocation(productFileLocation);
            }
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
            saveProcessedProductFiles(outputFilesNames, pm);
        }
    }

    public void saveProcessedProductFiles(String[] outputFilenames, ProgressMonitor pm) throws IOException {
        Configuration conf = getConfiguration();
        String executable = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR + parameterSuffix);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS + parameterSuffix);
        String tableOutputFilename = null;
        if (getInputParameters().length > 0) {
            final StringBuilder accu = processorParameters != null ? new StringBuilder(processorParameters) : new StringBuilder();
            for (int i=0; i<getInputParameters().length; i+=2) {
                if ("output".equals(getInputParameters()[i])) {
                    tableOutputFilename = getInputParameters()[i+1];
                } else if ("regionGeometry".equals(getInputParameters()[i])) {
                    conf.set("calvalus.regionGeometry", getInputParameters()[i + 1]);
                } else {
                    if (accu.length() > 0) {
                        accu.append('\n');
                    }
                    accu.append(getInputParameters()[i]);
                    accu.append('=');
                    accu.append(getInputParameters()[i + 1]);
                }
            }
            processorParameters = accu.toString();
        }

        ScriptGenerator scriptGenerator = new ScriptGenerator(ScriptGenerator.Step.FINALIZE, executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", conf);
        velocityContext.put("parameterText", processorParameters);
        velocityContext.put("parameters", PropertiesHandler.asProperties(processorParameters));

        velocityContext.put("outputFileNames", outputFilenames);
        Path outputPath = getOutputDirectoryPath();
        velocityContext.put("outputPath", outputPath);
        velocityContext.put("workOutputPath", getWorkOutputDirectoryPath());
        velocityContext.put("GlobalFunctions", new SnapGraphAdapter.GlobalFunctions(getLogger()));

        scriptGenerator.addScriptResources(conf, parameterSuffix);
        long t0 = System.currentTimeMillis();
        if (scriptGenerator.hasStepScript()) {
            scriptGenerator.writeScriptFiles(cwd, debugScriptGenerator);

            getLogger().info("finalize: " + executable + " " + Arrays.toString(outputFilenames) + " " + outputPath.toString());
            String[] cmdArray = new String[outputFilenames.length + 2];
            cmdArray[0] = "./finalize";
            System.arraycopy(outputFilenames, 0, cmdArray, 1, outputFilenames.length);
            cmdArray[cmdArray.length - 1] = outputPath.toString();
            String user = conf.get("mapreduce.job.user.name");
            String[] env = new String[] { "HADOOP_USER_NAME=" + user };

            Process process = Runtime.getRuntime().exec(cmdArray, env);
            String processLogName = executable + "-finalize";
            KeywordHandler keywordHandler = new KeywordHandler(processLogName, getMapContext());

            new ProcessObserver(process).
                    setName(processLogName).
                    setProgressMonitor(pm).
                    setHandler(keywordHandler).
                    start();
        } else {
            pm.beginTask("saving", 1);
            MapContext mapContext = getMapContext();
            final String outputCompression = conf.get(JobConfigNames.OUTPUT_COMPRESSION);
            if ("zip".equals(outputCompression) && outputFilenames.length == 1) {
                LOG.info("Creating ZIP archive on HDFS.");
                OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFilenames[0] + ".zip");
                ProductFormatter.zip(new File(outputFilenames[0]), outputStream, mapContext);
            } else if ("dir2".equals(outputCompression) && outputFilenames.length == 1) {
                LOG.info("Copying content of output dir to HDFS.");
                DirScanner dirScanner = new DirScanner(new File(outputFilenames[0]), true, true);
                String[] entryPaths = dirScanner.scan();
                for (String entryPath : entryPaths) {
                    File sourceFile = new File(new File(outputFilenames[0]).getAbsolutePath() + File.separator + entryPath);
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFile));
                    OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, entryPath);
                    ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
                }
            } else {
                for (String outputFileName : outputFilenames) {
                    InputStream is = new BufferedInputStream(new FileInputStream(new File(cwd, outputFileName)));
                    Path workPath = new Path(getWorkOutputDirectoryPath(),
                                             tableOutputFilename != null ? tableOutputFilename : outputFileName);
                    OutputStream os = workPath.getFileSystem(conf).create(workPath);
                    ProductFormatter.copyAndClose(is, os, mapContext);
                }
            }
            pm.done();
        }
        getLogger().info("archiving done in [ms]: " + (System.currentTimeMillis() - t0));
    }

    @Override
    public Path getOutputProductPath() throws IOException {
        if (outputFilesNames != null && outputFilesNames.length > 0) {
            return new Path(getWorkOutputDirectoryPath(), outputFilesNames[0]);
        }
        return null;
    }

    @Override
    public boolean supportsPullProcessing() {
        return false;
    }

}
