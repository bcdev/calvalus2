package com.bc.calvalus.production;


import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Information about a production.
 *
 * @author Norman
 */
public class Production {
    public static final SimpleDateFormat yyyyMMddHHmmss = new SimpleDateFormat("yyyyMMddHHmmss");
    private static long uniqueLong = System.nanoTime();
    private final String id;
    private final String name;
    private final WorkflowItem workflow;
    private final boolean autoStaging;
    private final ProductionRequest productionRequest;
    private final String outputPath;
    private final String[] intermediateDataPath;
    private final String stagingPath;
    private volatile ProcessStatus stagingStatus; // must be volatile, because staging is performed in separate threads

    public Production(String id,
                      String name,
                      String outputPath,
                      String stagingPath,
                      boolean autoStaging,
                      ProductionRequest productionRequest,
                      WorkflowItem workflow) {
        this(id, name, outputPath,
             allIntermediatePathes(outputPath, workflow),
             stagingPath, autoStaging, productionRequest, workflow);
    }

    public Production(String id,
                      String name,
                      String outputPath,
                      String[] intermediateDataPath,
                      String stagingPath,
                      boolean autoStaging,
                      ProductionRequest productionRequest,
                      WorkflowItem workflow) {
        if (id == null) {
            throw new NullPointerException("id");
        }
        if (name == null) {
            throw new NullPointerException("name");
        }
        if (intermediateDataPath == null) {
            throw new NullPointerException("intermediateDataPath");
        }
        if (productionRequest == null) {
            throw new NullPointerException("productionRequest");
        }
        this.id = id;
        this.name = name;  // todo - check: remove field, instead get from productionRequest.name (nf)
        this.outputPath = outputPath;
        this.intermediateDataPath = intermediateDataPath;
        this.stagingPath = stagingPath;
        this.autoStaging = autoStaging;
        this.workflow = workflow;
        this.productionRequest = productionRequest;
        this.stagingStatus = ProcessStatus.UNKNOWN;
    }

    public static String createId(String productionType) {
        return String.format("%s_%s_%8s",
                             yyyyMMddHHmmss.format(new Date()),
                             productionType,
                             Long.toHexString(nextUniqueLong()));
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public WorkflowItem getWorkflow() {
        return workflow;
    }

    public Object[] getJobIds() {
        return workflow.getJobIds();
    }

    public boolean isAutoStaging() {
        return autoStaging;
    }

    public ProductionRequest getProductionRequest() {
        return productionRequest;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String[] getIntermediateDataPath() {
        return intermediateDataPath;
    }

    public String getStagingPath() {
        return stagingPath;
    }

    public ProcessStatus getProcessingStatus() {
        return workflow.getStatus();
    }

    public void setProcessingStatus(ProcessStatus processingStatus) {
        workflow.setStatus(processingStatus);
    }

    public ProcessStatus getStagingStatus() {
        return stagingStatus;
    }

    public void setStagingStatus(ProcessStatus stagingStatus) {
        this.stagingStatus = stagingStatus;
    }

    private static long nextUniqueLong() {
        return ++uniqueLong;
    }

    @Override
    public String toString() {
        return "Production{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", stagingPath=" + stagingPath +
                ", processingStatus=" + getProcessingStatus() +
                ", stagingStatus=" + stagingStatus +
                '}';
    }

    static String[] allIntermediatePathes(String outputPath, WorkflowItem workflow) {
        Set<String> pathSet = new HashSet<>();
        accumulateOutputPathes(workflow, pathSet);
        pathSet.remove(outputPath);
        return pathSet.toArray(new String[0]);
    }

    private static void accumulateOutputPathes(WorkflowItem workflowItem, Set<String> pathSet) {
        if (workflowItem instanceof HadoopWorkflowItem) {
            HadoopWorkflowItem hadoopWorkflowItem = (HadoopWorkflowItem) workflowItem;
            String outputDir = hadoopWorkflowItem.getOutputDir();
            pathSet.add(outputDir);
        } else {
            for (WorkflowItem item : workflowItem.getItems()) {
                accumulateOutputPathes(item, pathSet);
            }
        }
    }
}
