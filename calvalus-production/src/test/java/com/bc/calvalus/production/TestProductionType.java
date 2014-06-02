package com.bc.calvalus.production;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowException;
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
        String userName = productionRequest.getUserName();
        productionCount++;
        Workflow.Parallel workflow = new Workflow.Parallel(new MyWorkflowItem(userName, "job_" + productionCount + "_1"),
                                                           new MyWorkflowItem(userName, "job_" + productionCount + "_2"));
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production("id_" + productionCount,
                              "name_" + productionCount,
                              "outputPath_" + productionCount,
                              "stagingPath_" + productionCount,
                              autoStaging,
                              productionRequest,
                              workflow);
    }

    @Override
    public Staging createStaging(Production production) throws ProductionException {
        Staging staging = new Staging() {
            @Override
            public void run() {
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

        private final String userName;

        MyWorkflowItem(String userName, String id) {
            super(id);
            this.userName = userName;
        }

        @Override
        public void submit() throws WorkflowException {
            // processingService.setJobStatus(id, new ProcessStatus(ProcessState.SCHEDULED));
        }

        @Override
        public void kill() throws WorkflowException {
            try {
                processingService.killJob(userName, getJobId());
            } catch (Exception e) {
                throw new WorkflowException(e);
            }
        }

        @Override
        public void updateStatus() {
            setStatus(processingService.getJobStatus(getJobId()));
        }
    }
}
