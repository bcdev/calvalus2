package com.bc.calvalus.binning;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface BinManager {
    // todo - add PropertyContext getInputPropertyContext()
    // todo - add PropertyContext getOutputPropertyContext()
    int getOutputPropertyCount();
    String getOutputPropertyName(int i);

    int getAggregatorCount();

    Aggregator getAggregator(int aggIndex);

    SpatialBin createSpatialBin(long binIndex);

    TemporalBin createTemporalBin(long binIndex);

    WritableVector createOutputVector();

    Vector getSpatialVector(SpatialBin bin, int aggIndex);

    Vector getTemporalVector(TemporalBin bin, int aggIndex);

    void aggregateSpatialBin(Observation obs, SpatialBin bin);

    void completeSpatialBin(SpatialBin bin);

    void aggregateTemporalBin(SpatialBin sBin, TemporalBin tBin);

    void computeOutput(TemporalBin temporalBin, WritableVector outputVector);

}
