package com.bc.calvalus.processing.l3;

import com.bc.calvalus.binning.*;

/**
 * Overrides the default implementation in order to create instances of {@link L3SpatialBin} and {@link L3TemporalBin}.
 *
 * @author Norman Fomferra
 */
public class L3BinManagerImpl extends BinManager {

    public L3BinManagerImpl(VariableContext variableContext, Aggregator... aggregators) {
        super(variableContext, aggregators);
    }

    @Override
    public L3SpatialBin createSpatialBin(long binIndex) {
        final L3SpatialBin spatialBin = new L3SpatialBin(binIndex, getSpatialFeatureCount());
        initSpatialBin(spatialBin);
        return spatialBin;
    }

    @Override
     public L3TemporalBin createTemporalBin(long binIndex) {
         final L3TemporalBin temporalBin = new L3TemporalBin(binIndex, getTemporalFeatureCount());
         initTemporalBin(temporalBin);
         return temporalBin;
     }
}
