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
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.runtime.internal.DirScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.zip.GZIPOutputStream;

/**
 * A processor adapter that uses a Python command line to process an input product.
 *
 * Instructions how to prepare the processor bundle:
 * 1. Create a working dir, put an input into it, add a processor python script name1.py <input>
 * 2. install required packages+conda-pack into a miniconda3-name2
 * 3. Activate the miniconda and run the processor with ./name1.py input
 * 4. conda pack miniconda3-name2.tar.gz
 * 5. Create bundles conda-name2-1.0 with the miniconda3-name2.tar.gz and python-name1-1.0 with the python script name1.py .
 * 6. Test with processing request (processorName:name1, condaEnv:miniconda3-name2, outputPattern:*.nc,
 *    checkIntersection:false, executableMemory:4096, processorBundles:conda-name2-1.0,python-name1-1.0)
 *
 * @author Martin
 */
public class PythonProcessorAdapter extends ProcessorAdapter {

    private String[] outputFilenames;

    public PythonProcessorAdapter(MapContext mapContext) {
        super(mapContext);
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {
        // make sure the input is staged into the working directory
        Path inputPath = getInputPath();
        File inputFile = getInputFile();
        if (inputFile == null) {
            if (inputPath.getName().endsWith(".zip")) {
                for (File entry : CalvalusProductIO.uncompressArchiveToCWD(inputPath, getConfiguration())) {
                    if ("xfdumanifest.xml".equals(entry.getName()) || entry.getName().endsWith("MTD.xml") || entry.getName().endsWith(".dim")) {
                        inputFile = entry;
                        break;
                    }
                }
                if (inputFile == null) {
                    inputFile = new File(inputPath.getName().substring(0, inputPath.getName().length()-4));
                }
            } else {
                inputFile = CalvalusProductIO.copyFileToLocal(inputPath, getConfiguration());
            }
            setInputFile(inputFile);
        }
        Path[] additionalInputPaths = getAdditionalInputPaths();
        if (additionalInputPaths != null) {
            for (Path additionalInputPath : additionalInputPaths) {
                if (additionalInputPath.getFileSystem(getConfiguration()).exists(additionalInputPath)) {
                    if (additionalInputPath.getName().endsWith(".zip")) {
                        CalvalusProductIO.uncompressArchiveToCWD(additionalInputPath, getConfiguration());
                    } else {
                        CalvalusProductIO.copyFileToLocal(additionalInputPath, getConfiguration()).getName();
                    }
                }
            }
        }
        // bookkeeping of bytes read
        if (getMapContext().getInputSplit() instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) getMapContext().getInputSplit();
            getMapContext().getCounter("Direct File System Counters", "FILE_SPLIT_BYTES_READ").setValue(fileSplit.getLength());
        }
        //
        Configuration conf = getConfiguration();
        final String user = conf.get("mapreduce.job.user.name");
        final String condaenvName = conf.get("calvalus.l2.condaenv");
        final String processorName = conf.get("calvalus.l2.operator");
        final String processorCallPattern = conf.get("calvalus.l2.commandline", processorName + ".py $input");
        final String outputPattern = conf.get("calvalus.output.pattern");
        final String memoryLimit = conf.get("mapreduce.map.memory.mb");
        final String inputLocalPath = inputFile.getCanonicalPath();
        final String processorCall = processorCallPattern.replace("$input", inputLocalPath.endsWith(".zip") ? inputLocalPath.substring(0, inputLocalPath.length()-4) : inputLocalPath);
        final String script =
                "set -x; " +
                String.format("let m=(%s * 1024); ulimit -m $m; ", memoryLimit) +
                String.format("e=$(ls -d */envs/%s|head -n 1); ", condaenvName) +
                "if [ \"$e\" != \"\" ]; then " +
                "  c=$(basename $(dirname $(dirname $e))); " +
                "  miniconda_dir=$(ls -ld $c|awk '{print $11}'); " +
                "  current_link=$(ls -ld /home/yarn/opt/$c 2> /dev/null | awk '{print $11}'); " +
                "  if [ \"$current_link\" != \"$miniconda_dir\" ]; then " +
                "    mkdir -p /home/yarn/opt; " +
                "    ln -s -f -T $miniconda_dir /home/yarn/opt/$c; " +
                "  fi; " +
                "  set -e; " +
                "  eval \"$(/home/yarn/opt/$c/bin/conda shell.bash hook)\"; " +
                "  conda activate ${e##*/}; " +
                "else " +
                "  set -e; " +
                String.format("  source %s/bin/activate; export PATH; ", condaenvName) +
                "fi; " +
                (inputLocalPath.endsWith(".zip") ? String.format("unzip %s; ", inputLocalPath) : "") +
                "export PYTHONPATH=.; " +
                String.format("./%s; ", processorCall) +
                String.format("for n in $(ls %s); do echo CALVALUS_OUTPUT_PRODUCT $n; done", outputPattern);
        getLogger().info("script=" + script);
        final String[] cmdArray = {"/bin/bash", "-c", script};
        final String[] env = new String[] { "HADOOP_USER_NAME=" + user };
        Process process = Runtime.getRuntime().exec(cmdArray, env);
        KeywordHandler keywordHandler = new KeywordHandler(processorName, getMapContext());

        new ProcessObserver(process).
                setName(processorName).
                setProgressMonitor(pm).
                setHandler(keywordHandler).
                start();

        outputFilenames = keywordHandler.getOutputFiles();
        return outputFilenames.length > 0;
    }

    @Override
    public Product openProcessedProduct() throws IOException {
        if (outputFilenames != null && outputFilenames.length > 0) {
            Product product = ProductIO.readProduct(new File(".", outputFilenames[0]));
            CalvalusProductIO.printProductOnStdout(product, "executable output");
            File productFileLocation = product.getFileLocation();
            if (isSentinel2(outputFilenames[0])) {
                Map<String, Object> params = new HashMap<>();
                params.put("referenceBand", "B5");
                product = GPF.createProduct("Resample", params, product);
                CalvalusProductIO.printProductOnStdout(product, "resampled");
                product.setFileLocation(productFileLocation);
            } else if (isLandsat(outputFilenames[0])) {
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
        if (outputFilenames != null && outputFilenames.length > 0) {
            saveProcessedProductFiles(outputFilenames, pm);
        }
    }

    protected void saveProcessedProductFiles(String[] outputFilesNames, ProgressMonitor pm) throws IOException {
        final Configuration conf = getConfiguration();
        final String outputCompression = conf.get(JobConfigNames.OUTPUT_COMPRESSION);
        String tableOutputFilename = null;
        if (getInputParameters().length > 0) {
            for (int i=0; i<getInputParameters().length; i+=2) {
                if ("output".equals(getInputParameters()[i])) {
                    tableOutputFilename = getInputParameters()[i+1];
                }
            }
        }
        pm.beginTask("saving", 1);
        MapContext mapContext = getMapContext();
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
            for (String outputFileName : outputFilesNames) {
                InputStream is = new BufferedInputStream(new FileInputStream(new File(".", outputFileName)));
                Path workPath = new Path(getWorkOutputDirectoryPath(),
                                         tableOutputFilename != null ? tableOutputFilename : outputFileName);
                OutputStream os = workPath.getFileSystem(conf).create(workPath);
                ProductFormatter.copyAndClose(is, os, mapContext);
            }
        }
        pm.done();
    }

    @Override
    public Path getOutputProductPath() throws IOException {
        if (outputFilenames != null && outputFilenames.length > 0) {
            return new Path(getWorkOutputDirectoryPath(), outputFilenames[0]);
        }
        return null;
    }

    @Override
    public boolean supportsPullProcessing() {
        return false;
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

}
