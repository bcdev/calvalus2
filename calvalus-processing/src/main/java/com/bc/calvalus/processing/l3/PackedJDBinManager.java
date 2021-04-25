package com.bc.calvalus.processing.l3;

import org.esa.snap.binning.Aggregator;
import org.esa.snap.binning.CellProcessorConfig;
import org.esa.snap.binning.VariableContext;

/**
 * Factory for PackedJDSpatialBin objects, specific bin for JD aggregator for Fire-S2
 *
 * @author Martin Boettcher
 */
public class PackedJDBinManager extends HadoopBinManager {
    public PackedJDBinManager(VariableContext variableContext, CellProcessorConfig postProcessorConfig, Aggregator... aggregators) {
        super(variableContext, postProcessorConfig, aggregators);
    }

    @Override
    public L3SpatialBin createSpatialBin(long binIndex) {
        final PackedJDSpatialBin spatialBin = new PackedJDSpatialBin(binIndex, getSpatialFeatureCount(), getGrowableAggregatorCount());
        initSpatialBin(spatialBin);
        return spatialBin;
    }
}
