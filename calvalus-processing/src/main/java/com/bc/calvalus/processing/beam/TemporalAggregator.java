package com.bc.calvalus.processing.beam;

import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.datamodel.Product;

import java.io.IOException;

/**
 * TemporalAggregator is used in tempagg production types. A series of input products is supplied incrementally.
 * Each product can be accessed before the aggregate call and closed after it has been aggregated
 * to avoid keeping all inputs open/stored during the complete aggregation.
 *
 * @author MB
 */
public interface TemporalAggregator {

    /** init is called once with the first input and may initialize data structures for accumulation. */
    void initialize(Configuration conf, Product firstInput);

    /** aggregate is called once per input. The first input is provided to init and to aggregate. */
    void aggregate(Product contribution) throws IOException;

    /** complete is called after the last input. It shall return the aggregated product. */
    Product complete();
}
