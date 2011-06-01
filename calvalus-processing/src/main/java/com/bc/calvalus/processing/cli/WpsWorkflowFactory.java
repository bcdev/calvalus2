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

package com.bc.calvalus.processing.cli;

import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.shellexec.FileUtil;

import java.io.IOException;

/**
 * Abstract base class for creating a workflow item from a WPS configuration.
 */
public abstract class WpsWorkflowFactory implements WorkflowFactory {

    @Override
    public WorkflowItem create(HadoopProcessingService hps, String[] args) {
        String requestFileName = args[0];
        WpsConfig wpsConfig = null;
        try {
            wpsConfig = getWpsConfig(args[1]);
        } catch (IOException e) {
            System.err.println("Failed to read request file: '" + requestFileName + "' " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        return create(hps, wpsConfig);
    }

    public abstract WorkflowItem create(HadoopProcessingService hps, WpsConfig wpsConfig);

    private static WpsConfig getWpsConfig(String requestPath) throws IOException {
        String requestContent = FileUtil.readFile(requestPath);
        return new WpsConfig(requestContent);
    }
}
