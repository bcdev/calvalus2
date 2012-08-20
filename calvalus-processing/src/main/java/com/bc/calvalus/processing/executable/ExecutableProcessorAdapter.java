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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.awt.Rectangle;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

/**
 * A processor adapter that uses an executable to process an input product.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorAdapter extends ProcessorAdapter {

    private static final String VM_SUFFIX = ".vm";
    private static final String CMDLINE = "cmdline";
    private File[] outputFiles;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        super(mapContext);
    }

    //TODO maybe return number of processed products
    @Override
    public boolean processSourceProduct(Rectangle srcProductRect) throws IOException {
        Configuration configuration = getConfiguration();
        String bundle = configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        String executable = configuration.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String programName = bundle + "-" + executable;
        File cwd = new File(".");

        Path inputPath = getInputPath();
        File inputDir = new File(cwd, "input");
        File outputDir = new File(cwd, "output");
        inputDir.mkdirs();
        outputDir.mkdirs();
        File inputFile = new File(inputDir, inputPath.getName());
        if (!inputFile.exists()) {
            FileSystem fs = FileSystem.get(configuration);
            FileUtil.copy(fs, inputPath, inputFile, false, configuration);
        }
        TemplateProcessor templateProcessor = new TemplateProcessor();
        templateProcessor.velocityContext.put("system", System.getProperties());
        templateProcessor.velocityContext.put("configuration", configuration);
        templateProcessor.velocityContext.put("inputFile", inputFile);
        templateProcessor.velocityContext.put("outputDir", outputDir);

        String cmdLine = processVelocityTemplates(templateProcessor, cwd, executable);
        Process process = Runtime.getRuntime().exec(cmdLine);
        ProgressMonitor pm = new ProductSplitProgressMonitor(getMapContext()); // TODO
        final ProcessObserver processObserver = new ProcessObserver(process, programName, pm);
        KeywordHandler keywordHandler = new KeywordHandler(programName, getMapContext(), pm);
        processObserver.addHandler(keywordHandler);
        processObserver.addHandler(new LogHandler(programName, CalvalusLogger.getLogger()));
        processObserver.startAndWait();
        pm.done();

        final int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException(programName + " failed with exit code " + exitCode + ".\nCheck log for more details.");
        }
        String[] outputFilesNames = keywordHandler.getOutputFiles();
        outputFiles = new File[outputFilesNames.length];
        for (int i = 0; i < outputFilesNames.length; i++) {
            outputFiles[i] = new File(cwd, outputFilesNames[i]);
        }
        return true;
    }

    private String processVelocityTemplates(TemplateProcessor templateProcessor, File cwd, final String executable) throws IOException {
        File[] vmFiles = cwd.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(executable);
            }
        });
        String cmdline = "";
        if (vmFiles != null) {
            for (File vmFile : vmFiles) {
                String filename = vmFile.getName();
                String outputFilename = createResultName(filename, executable);
                File outputFile = new File(cwd, outputFilename);
                if (filename.endsWith(VM_SUFFIX)) {
                    templateProcessor.process(vmFile, outputFile);
                } else {
                    FileUtil.symLink(filename, outputFilename);
                }
                if (outputFilename.equals(CMDLINE)) {
                    cmdline = FileUtils.readText(outputFile).trim();
                }
            }
        }
        return cmdline;
    }

    static String createResultName(String name, String executable) {
        name = name.substring(executable.length() + 1); // strip executable name from the front
        if (name.endsWith(VM_SUFFIX)) {
            name = name.substring(0, name.length() - VM_SUFFIX.length()); // strip .vm from the end
        }
        return name;
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFiles != null && outputFiles.length > 0) {
            return ProductIO.readProduct(outputFiles[0]);
        }
        return null;
    }

    @Override
    public void saveProcessedProducts() throws Exception {
        if (outputFiles != null && outputFiles.length > 0) {
            MapContext mapContext = getMapContext();
            for (File outputFile : outputFiles) {
                InputStream inputStream = new BufferedInputStream(new FileInputStream(outputFile));
                OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFile.getName());
                ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
            }
        }
    }

    static class TemplateProcessor {
        final VelocityEngine velocityEngine;
        final VelocityContext velocityContext;

        TemplateProcessor() {
            velocityEngine = new VelocityEngine();
            try {
                velocityEngine.init();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to initialize velocity engine", e);
            }
            velocityContext = new VelocityContext();
        }

        public void process(File vmFile, File outputFile) throws IOException {
            Reader inputReader = new FileReader(vmFile);
            Writer outputWriter = new FileWriter(outputFile);
            try {
                velocityEngine.evaluate(velocityContext, outputWriter, "ExecutableProcessorAdapter", inputReader);
            } finally {
                outputWriter.close();
                inputReader.close();
            }
        }
    }
}
