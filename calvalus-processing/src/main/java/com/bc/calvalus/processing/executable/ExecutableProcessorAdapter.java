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
import com.bc.ceres.resource.FileResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.velocity.VelocityContext;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.Product;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A processor adapter that uses an executable to process an input product.
 *
 * @author MarcoZ
 */
public class ExecutableProcessorAdapter extends ProcessorAdapter {

    private File[] outputFiles;

    public ExecutableProcessorAdapter(MapContext mapContext) {
        super(mapContext);
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("Exec Level 2");
        Configuration configuration = getConfiguration();
        String bundle = configuration.get(JobConfigNames.CALVALUS_L2_BUNDLE);
        final String executable = configuration.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String programName = bundle + "-" + executable;
        File cwd = new File(".");

        File inputFile = copyProductToLocal(getInputPath());
        File outputDir = new File(cwd, "output");
        outputDir.mkdirs();

        ScriptGenerator scriptGenerator = new ScriptGenerator(executable);
        VelocityContext velocityContext = scriptGenerator.getVelocityContext();
        velocityContext.put("system", System.getProperties());
        velocityContext.put("configuration", configuration);
        velocityContext.put("inputFile", inputFile);
        velocityContext.put("outputDir", outputDir);

        addScriptResources(cwd, scriptGenerator, executable);
        scriptGenerator.writeScripts(cwd);

        Process process = Runtime.getRuntime().exec(scriptGenerator.getCommandLine());
        KeywordHandler keywordHandler = new KeywordHandler(programName, getMapContext());

        new ProcessObserver(process).
                setName(programName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();

        String[] outputFilesNames = keywordHandler.getOutputFiles();
        outputFiles = new File[outputFilesNames.length];
        for (int i = 0; i < outputFilesNames.length; i++) {
            outputFiles[i] = new File(cwd, outputFilesNames[i]);
        }
        return outputFiles.length;
    }

    private void addScriptResources(File cwd, ScriptGenerator scriptGenerator, final String executable) {
        File[] vmFiles = cwd.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(executable);
            }
        });
        if (vmFiles != null) {
            for (File vmFile : vmFiles) {
                System.out.println("adding Resource = " + vmFile);
                scriptGenerator.addResource(new FileResource(vmFile.getName()));
            }
        }
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFiles != null && outputFiles.length > 0) {
            return ProductIO.readProduct(outputFiles[0]);
        }
        return null;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws Exception {
        if (outputFiles != null && outputFiles.length > 0) {
            MapContext mapContext = getMapContext();
            for (File outputFile : outputFiles) {
                InputStream inputStream = new BufferedInputStream(new FileInputStream(outputFile));
                OutputStream outputStream = ProductFormatter.createOutputStream(mapContext, outputFile.getName());
                ProductFormatter.copyAndClose(inputStream, outputStream, mapContext);
            }
        }
    }

}
