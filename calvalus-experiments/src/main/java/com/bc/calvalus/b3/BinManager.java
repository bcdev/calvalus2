package com.bc.calvalus.b3;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public interface BinManager {

    int getAggregatorCount();

    Aggregator getAggregator(int aggIndex);

    SpatialBin createSpatialBin(int binIndex);

    TemporalBin createTemporalBin(int binIndex);

    WritableVector createOutputVector();

    Vector getSpatialVector(SpatialBin bin, int aggIndex);

    Vector getTemporalVector(TemporalBin bin, int aggIndex);

    void aggregateSpatialBin(Observation obs, SpatialBin bin);

    void completeSpatialBin(SpatialBin bin);

    void aggregateTemporalBin(SpatialBin sBin, TemporalBin tBin);

    void computeOutput(TemporalBin temporalBin, WritableVector outputVector);
}
