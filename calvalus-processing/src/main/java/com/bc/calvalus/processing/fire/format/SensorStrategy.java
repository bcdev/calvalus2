package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.Workflow;

public interface SensorStrategy {

    PixelProductArea getArea(String identifier);

    PixelProductArea[] getAllAreas();

    Workflow getPixelFormattingWorkflow(WorkflowConfig workflowConfig);

    interface PixelProductAreaProvider {

        PixelProductArea getArea(String identifier);

        PixelProductArea[] getAllAreas();
    }
}
