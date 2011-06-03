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

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.vividsolutions.jts.geom.Geometry;

import java.io.IOException;

/**
 * Level 2 workflow item factory.
 */
public class L2WorkflowFactory implements WorkflowFactory {

    @Override
    public String getName() {
        return "l2";
    }

    @Override
    public String getUsage() {
        return "l2 <wpsFile.xml>  -- 'Level 2 processing'";
    }

    @Override
    public WorkflowItem create(HadoopProcessingService hps, String[] args) throws WorkflowException {
        WpsConfig wps;
        try {
            wps = WpsConfig.createFromFile(args[0]);
        } catch (IOException e) {
            throw new WorkflowException("Failed to read request file: '" + args[0] + "' " + e.getMessage(), e);
        }

        Geometry geometry = JobUtils.createGeometry(wps.getGeometry());

        return new L2WorkflowItem(hps,
                                  wps.getIdentifier(),
                                  wps.getProcessorPackage(),
                                  wps.getOperatorName(),
                                  wps.getLevel2Parameters(),
                                  geometry,
                                  wps.getRequestInputPaths(),
                                  wps.getRequestOutputDir());
    }
}
