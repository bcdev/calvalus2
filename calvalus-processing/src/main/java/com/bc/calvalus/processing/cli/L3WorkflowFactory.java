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
import com.bc.calvalus.processing.l3.L3Config;
import com.bc.calvalus.processing.l3.L3WorkflowItem;
import com.vividsolutions.jts.geom.Geometry;

import java.io.IOException;

/**
 * Level 3 workflow item factory.
 */
public class L3WorkflowFactory implements WorkflowFactory {

    @Override
    public String getName() {
        return "l3";
    }

    @Override
    public String getUsage() {
        return "l3 <wpsFile.xml>  -- 'Level 3 processing'";
    }

    @Override
    public WorkflowItem create(HadoopProcessingService hps, String[] args) throws WorkflowException {
        WpsConfig wps;
        try {
            wps = WpsConfig.createFromFile(args[0]);
        } catch (IOException e) {
            throw new WorkflowException("Failed to read request file: '" + args[0] + "' " + e.getMessage(), e);
        }

        L3Config l3Config = L3Config.fromXml(wps.getLevel3Parameters());
        Geometry geometry = JobUtils.createGeometry(wps.getGeometry());
        //dates are not used for the l3 processing
        String minDate = "";
        String maxDate = "";
        return new L3WorkflowItem(hps,
                                  wps.getIdentifier(),
                                  wps.getProcessorPackage(),
                                  wps.getOperatorName(),
                                  wps.getLevel2Parameters(),
                                  geometry,
                                  wps.getRequestInputPaths(),
                                  wps.getRequestOutputDir(),
                                  l3Config,
                                  minDate,
                                  maxDate);
    }

}
