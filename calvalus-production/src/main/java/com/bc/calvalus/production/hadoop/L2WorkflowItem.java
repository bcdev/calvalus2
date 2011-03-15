/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.WorkflowException;
import com.bc.calvalus.processing.beam.BeamOpProcessingType;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.production.ProductionException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.JobID;

/**
* Created by IntelliJ IDEA.
* User: Norman
* Date: 15.03.11
* Time: 16:54
* To change this template use File | Settings | File Templates.
*/
public class L2WorkflowItem extends HadoopWorkflowItem {

    private final ProcessingRequest processingRequest;

    public L2WorkflowItem(HadoopProcessingService processingService, ProcessingRequest processingRequest) {
        super(processingService);
        this.processingRequest = processingRequest;
    }

    @Override
    public void submit() throws WorkflowException {
        try {
            final JobClient jobClient = getProcessingService().getJobClient();
            final BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
            JobID jobId = beamOpProcessingType.submitJob(processingRequest.getProcessingParameters());
            setJobId(jobId);
        } catch (Exception e) {
            throw new WorkflowException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }
}
