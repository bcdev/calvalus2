package com.bc.calvalus.processing.ma;

/**
 * A filter for aggregated numbers.
 *
 * @author Norman
 */
public interface AggregatedNumberFilter {
    boolean accept(int attributeIndex, AggregatedNumber number);
}
