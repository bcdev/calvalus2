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

import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.mapreduce.MapContext;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handler that tries to extract keywords from stdout of a processor
 */
class KeywordHandler implements ProcessObserver.Handler {

    private final static String PROGRESS_REGEX = "CALVALUS_PROGRESS ([0-9\\.]+)";
    private final static String PRODUCT_REGEX = "CALVALUS_OUTPUT_PRODUCT (.+)$";
    private final String programName;
    private final MapContext mapContext;
    private final ProgressMonitor pm;
    private final Pattern progressPattern;
    private final Pattern productPattern;
    private final List<String> outputFiles;

    private boolean progressSeen;
    private int lastScan = 0;


    KeywordHandler(String programName, MapContext mapContext, ProgressMonitor pm) {
        this.programName = programName;
        this.mapContext = mapContext;
        this.pm = pm;
        this.progressPattern = Pattern.compile(PROGRESS_REGEX);
        this.productPattern = Pattern.compile(PRODUCT_REGEX);
        this.outputFiles = new ArrayList<String>();
    }


    public void handleLineOnStdoutRead(String line) {
        mapContext.progress(); //trigger progress
        if (!progressSeen) {
            progressSeen = true;
            pm.beginTask(programName, 1000);
        }

        Matcher progressMatcher = progressPattern.matcher(line);
        if (progressMatcher.find()) {
            float progressValue = Float.parseFloat(progressMatcher.group(1));

            int scan = (int) (progressValue * 1000);
            pm.worked(scan - lastScan);
            lastScan = scan;
        } else {
            Matcher productMatcher = productPattern.matcher(line);
            if (productMatcher.find()) {
                outputFiles.add(productMatcher.group(1).trim());
            }
        }
    }

    @Override
    public void handleLineOnStderrRead(String line) {
        mapContext.progress();
    }

    public String[] getOutputFiles() {
        return outputFiles.toArray(new String[outputFiles.size()]);
    }
}
