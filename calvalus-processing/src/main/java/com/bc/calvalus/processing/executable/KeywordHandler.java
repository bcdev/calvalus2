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

import com.bc.ceres.core.ProcessObserver;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.mapreduce.MapContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handler that tries to extract keywords from stdout of a processor
 */
public class KeywordHandler extends ProcessObserver.DefaultHandler {

    private final static String KEYWORD_PREFIX = "CALVALUS";
    private final static String PROGRESS_REGEX = "CALVALUS_PROGRESS ([0-9\\.]+)";
    private final static String INPUT_PRODUCT_REGEX = "CALVALUS_INPUT_PRODUCT (.+)$";
    private final static String OUTPUT_PRODUCT_REGEX = "CALVALUS_OUTPUT_PRODUCT (.+)$";
    private final static String NAMED_OUTPUT_PRODUCT_REGEX = "CALVALUS_NAMED_OUTPUT_PRODUCT\\s+(\\S+)\\s+(\\S+)$";
    private final static String PRODUCT_TRANSFORMATION = "CALVALUS_PRODUCT_TRANSFORMATION (.+)$";

    private final String programName;
    private final MapContext mapContext;

    protected Pattern progressPattern;
    protected Pattern outputProductPattern;
    protected Pattern namedOutputProductPattern;
    protected Pattern inputProductPattern;
    protected Pattern productTransformationPattern;

    protected final List<String> outputFiles;
    private final List<NamedOutput> namedOutputFiles;
    private String inputFile = null;
    private int lastScan = 0;
    private boolean skipProcessing = false;
    private String productTransformation = null;


    public KeywordHandler(String programName, MapContext mapContext) {
        this.programName = programName;
        this.mapContext = mapContext;

        this.progressPattern = Pattern.compile(PROGRESS_REGEX);
        this.outputProductPattern = Pattern.compile(OUTPUT_PRODUCT_REGEX);
        this.namedOutputProductPattern = Pattern.compile(NAMED_OUTPUT_PRODUCT_REGEX);
        this.inputProductPattern = Pattern.compile(INPUT_PRODUCT_REGEX);
        this.productTransformationPattern = Pattern.compile(PRODUCT_TRANSFORMATION);

        this.outputFiles = new ArrayList<String>();
        this.namedOutputFiles = new ArrayList<NamedOutput>();
    }

    @Override
    public void onObservationStarted(ProcessObserver.ObservedProcess process, ProgressMonitor pm) {
        super.onObservationStarted(process, pm);
        pm.beginTask(programName, 1000);
    }

    @Override
    public void onStdoutLineReceived(ProcessObserver.ObservedProcess process, String line, ProgressMonitor pm) {
        super.onStdoutLineReceived(process, line, pm);
        mapContext.progress(); //signal activity to Hadoop

        if (line.startsWith(KEYWORD_PREFIX)) {
            Matcher progressMatcher = progressPattern.matcher(line);
            if (progressMatcher.find()) {
                float progressValue = Float.parseFloat(progressMatcher.group(1));

                int scan = (int) (progressValue * 1000);
                pm.worked(scan - lastScan);
                lastScan = scan;
                return;
            }
            Matcher outputProductMatcher = outputProductPattern.matcher(line);
            if (outputProductMatcher.find()) {
                outputFiles.add(outputProductMatcher.group(1).trim());
                return;
            }
            Matcher inputProductMatcher = inputProductPattern.matcher(line);
            if (inputProductMatcher.find()) {
                inputFile = inputProductMatcher.group(1).trim();
                return;
            }
            if (line.equalsIgnoreCase("CALVALUS_SKIP_PROCESSING yes")) {
                skipProcessing = true;
                return;
            }
            Matcher namedOutputProductMatcher = namedOutputProductPattern.matcher(line);
            if (namedOutputProductMatcher.find()) {
                String name = namedOutputProductMatcher.group(1).trim();
                String file = namedOutputProductMatcher.group(2).trim();
                namedOutputFiles.add(new NamedOutput(name, file));
            }
            Matcher productTransformationMatcher = productTransformationPattern.matcher(line);
            if (productTransformationMatcher.find()) {
                productTransformation = productTransformationMatcher.group(1).trim();
                return;
            }
        }
    }

    @Override
    public void onStderrLineReceived(ProcessObserver.ObservedProcess process, String line, ProgressMonitor pm) {
        super.onStderrLineReceived(process, line, pm);
        mapContext.progress(); //signal activity to Hadoop
    }

    @Override
    public void onObservationEnded(ProcessObserver.ObservedProcess process, Integer exitCode, ProgressMonitor pm) {
        super.onObservationEnded(process, exitCode, pm);
        pm.done();
        if (exitCode == null || exitCode != 0) {
            throw new RuntimeException(programName + " failed with exit code " + exitCode + ".\nCheck log for more details.");
        }
    }

    public String getInputFile() {
        return inputFile;
    }

    public String[] getOutputFiles() {
        return outputFiles.toArray(new String[outputFiles.size()]);
    }

    public NamedOutput[] getNamedOutputFiles() {
        return namedOutputFiles.toArray(new NamedOutput[namedOutputFiles.size()]);
    }

    public boolean skipProcessing() {
        return skipProcessing;
    }

    public String getProductTransformation() {
        return productTransformation;
    }

    public static class NamedOutput {
        private final String name;
        private final String file;

        private NamedOutput(String name, String file) {
            this.name = name;
            this.file = file;
        }

        public String getName() {
            return name;
        }

        public String getFile() {
            return file;
        }
    }
}
