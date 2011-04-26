package com.bc.calvalus.binning;

import com.bc.ceres.binding.PropertyDescriptor;
import com.bc.ceres.binding.PropertySet;

import java.util.Map;

/**
 * A descriptor for aggregators.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface AggregatorDescriptor {
    String getName();

    PropertyDescriptor[] getParameterDescriptors();

    Aggregator createAggregator(VariableContext varCtx, PropertySet configuration);
}
