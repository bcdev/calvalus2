package com.bc.calvalus.processing.hadoop;


import org.esa.beam.framework.datamodel.MetadataElement;

public interface MetadataAggregator {

    public void aggregate(MetadataElement metadataElement);

    public MetadataElement getAggregatedMetadata();
}
