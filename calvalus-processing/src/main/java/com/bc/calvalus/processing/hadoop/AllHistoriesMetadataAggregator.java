package com.bc.calvalus.processing.hadoop;

import org.esa.beam.framework.datamodel.MetadataElement;

public class AllHistoriesMetadataAggregator implements MetadataAggregator {

    private final MetadataElement sourceProducts;

    public AllHistoriesMetadataAggregator() {
        sourceProducts = new MetadataElement("source_products");
    }

    @Override
    public void aggregate(MetadataElement metadataElement) {
        sourceProducts.addElement(metadataElement);
    }

    @Override
    public MetadataElement getAggregatedMetadata() {
        return sourceProducts;
    }
}
