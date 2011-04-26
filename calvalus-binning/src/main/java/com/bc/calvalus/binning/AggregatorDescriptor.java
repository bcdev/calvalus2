package com.bc.calvalus.binning;

/**
 * A descriptor for aggregators.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface AggregatorDescriptor {
    Aggregator createAggregator();
}
