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
* Time: 16:56
* To change this template use File | Settings | File Templates.
*/
class L3WorkflowItem extends HadoopWorkflowItem {

    private final String productionId;
    private final String productionName;
    private final WpsXmlGenerator wpsXmlGenerator;
    private final L3ProcessingRequest processingRequest;

    public L3WorkflowItem(HadoopProcessingService processingService,
                          WpsXmlGenerator wpsXmlGenerator,
                          String productionId,
                          String productionName,
                          L3ProcessingRequest processingRequest) {
        super(processingService);
        this.wpsXmlGenerator = wpsXmlGenerator;
        this.productionId = productionId;
        this.productionName = productionName;
        this.processingRequest = processingRequest;
    }

    @Override
    public void submit() throws ProductionException {
        try {
            String wpsXml = wpsXmlGenerator.createL3WpsXml(productionId, productionName, processingRequest);
            JobClient jobClient = getProcessingService().getJobClient();
            BeamOpProcessingType beamOpProcessingType = new BeamOpProcessingType(jobClient);
            JobID jobId = beamOpProcessingType.submitJob(wpsXml);
            setJobId(jobId);
        } catch (Exception e) {
            throw new ProductionException("Failed to submit Hadoop job: " + e.getMessage(), e);
        }
    }
}
