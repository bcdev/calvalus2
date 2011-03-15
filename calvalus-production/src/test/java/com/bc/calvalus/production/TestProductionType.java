package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.processing.ProcessingService;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.junit.Ignore;

import java.io.IOException;

@Ignore
public class TestProductionType implements ProductionType {
    int productionCount;
    private final ProcessingService<String> processingService;
    private final StagingService stagingService;

    public TestProductionType(ProcessingService<String> processingService, StagingService stagingService) {
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        productionCount++;
        Workflow.Parallel workflow = new Workflow.Parallel(new MyWorkflowItem("job_" + productionCount + "_1"),
                                                           new MyWorkflowItem("job_" + productionCount + "_2"));
        return new Production("id_" + productionCount,
                              "name_" + productionCount,
                              "user_" + productionCount,
                              "stagingPath_" + productionCount,
                              productionRequest,
                              workflow);
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        Staging staging = new Staging() {

            @Override
            public String call() throws Exception {
                return null;
            }

        };
        try {
            stagingService.submitStaging(staging);
        } catch (IOException e) {
            throw new ProductionException(e);
        }
        return staging;
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return "test".equals(productionRequest.getProductionType());
    }

    /**
     * A simple WorkflowItem that does nothing on submit(), but uses the given ProcessingService
     * to kill() and updateStatus().
     */
    public class MyWorkflowItem extends TestWorkflowItem<String> {

        MyWorkflowItem(String id) {
            super(id);
        }

        @Override
        public void submit() throws ProductionException {
            // processingService.setJobStatus(id, new ProcessStatus(ProcessState.SCHEDULED));
        }

        @Override
        public void kill() throws ProductionException {
            try {
                processingService.killJob(getJobId());
            } catch (IOException e) {
                throw new ProductionException(e);
            }
        }

        @Override
        public void updateStatus() {
            ProcessStatus jobStatus = processingService.getJobStatus(getJobId());
            if (jobStatus != null) {
                setStatus(jobStatus);
            }
        }
    }
}
