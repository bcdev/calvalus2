package com.bc.calvalus.processing.l3.binprocessing;

import org.esa.beam.binning.AggregatorConfig;

/**
 * Configuration of a binning processor.
 */
public abstract class BinProcessorConfig extends AggregatorConfig {

    public BinProcessorConfig(String aggregatorName) {
        this.type = aggregatorName;
    }

}
