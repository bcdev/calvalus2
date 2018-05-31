package com.bc.calvalus.processing.fire.format;

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.processing.fire.format.grid.avhrr.AvhrrGridInputFormat;
import com.bc.calvalus.processing.fire.format.grid.avhrr.AvhrrGridMapper;
import com.bc.calvalus.processing.fire.format.grid.avhrr.AvhrrGridReducer;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AvhrrStrategy implements SensorStrategy {

    public AvhrrStrategy() {
    }

    @Override
    public PixelProductArea getArea(String identifier) {
        throw new NotImplementedException("Not needed");
    }

    @Override
    public PixelProductArea[] getAllAreas() {
        throw new NotImplementedException("Not needed");
    }

    @Override
    public Workflow getPixelFormattingWorkflow(WorkflowConfig workflowConfig) {
        throw new NotImplementedException("Not needed");
    }

    @Override
    public Class<? extends InputFormat> getGridInputFormat() {
        return AvhrrGridInputFormat.class;
    }

    @Override
    public Class<? extends Mapper> getGridMapperClass() {
        return AvhrrGridMapper.class;
    }

    @Override
    public Class<? extends Reducer> getGridReducerClass() {
        return AvhrrGridReducer.class;
    }
}
