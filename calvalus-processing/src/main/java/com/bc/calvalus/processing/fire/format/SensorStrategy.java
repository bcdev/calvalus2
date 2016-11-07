package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.Workflow;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public interface SensorStrategy {

    PixelProductArea getArea(String identifier);

    PixelProductArea[] getAllAreas();

    Workflow getPixelFormattingWorkflow(WorkflowConfig workflowConfig);

    Class<? extends InputFormat> getGridInputFormat();

    Class<? extends Mapper> getGridMapperClass();

    Class<? extends Reducer> getGridReducerClass();

    interface PixelProductAreaProvider {

        PixelProductArea getArea(String identifier);

        PixelProductArea[] getAllAreas();
    }
}
