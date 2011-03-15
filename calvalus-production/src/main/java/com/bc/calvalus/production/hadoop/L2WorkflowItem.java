package com.bc.calvalus.production.hadoop;

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
    public void submit() throws ProductionException {
        try {
            final JobClient jobClient = getProcessingService().getJobClient();
            final BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
            JobID jobId = beamOpProcessingType.submitJob(processingRequest.getProcessingParameters());
            setJobId(jobId);
        } catch (Exception e) {
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }
}
