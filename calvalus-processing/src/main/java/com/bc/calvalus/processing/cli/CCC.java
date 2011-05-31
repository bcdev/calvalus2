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
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.WpsConfig;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l2.L2WorkflowItem;
import com.bc.calvalus.processing.shellexec.FileUtil;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * The Calalus Commandline Client.
 */
public class CCC {
    private HadoopProcessingService hps;

    public static void main(String[] args) throws IOException, WorkflowException {
        String requestPath = args[0];
        String requestContent = FileUtil.readFile(requestPath);
        WpsConfig wps = new WpsConfig(requestContent);
        CCC ccc = new CCC();
        L2WorkflowItem workflowItem = ccc.createL2(wps);
        workflowItem.submit();
    }

    public CCC() throws IOException {
        JobConf jobConf = new JobConf();
        //TODO make this configurable
        jobConf.set("hadoop.fs.default.name", "hdfs://cvmaster00:9000");
        jobConf.set("hadoop.mapred.job.tracker", "cvmaster00:9001");
        JobClient jobClient = new JobClient(jobConf);
        hps = new HadoopProcessingService(jobClient);
    }

    private L2WorkflowItem createL2(WpsConfig wps) {
        return new L2WorkflowItem(hps,
                                  wps.getIdentifier(),
                                  wps.getProcessorPackage(),
                                  wps.getOperatorName(),
                                  wps.getLevel2Parameters(),
                                  JobUtils.createGeometry(wps.getGeometry()),
                                  wps.getRequestInputPaths(),
                                  wps.getRequestOutputDir());
    }
}
