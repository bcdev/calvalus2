package com.bc.calvalus.binning;

/**
 * The bin manager class comprises a number of {@link Aggregator}s
 *
 * @author Norman Fomferra
 */
public interface BinManager {

    String[] getOutputFeatureNames();

    double getOutputFeatureFillValue(int i);

    int getAggregatorCount();

    Aggregator getAggregator(int aggIndex);

    SpatialBin createSpatialBin(long binIndex);

    TemporalBin createTemporalBin(long binIndex);

    WritableVector createOutputVector();

    Vector getSpatialVector(SpatialBin bin, int aggIndex);

    Vector getTemporalVector(TemporalBin bin, int aggIndex);

    void aggregateSpatialBin(Observation obs, SpatialBin outputBin);

    void completeSpatialBin(SpatialBin bin);

    void aggregateTemporalBin(SpatialBin inputBin, TemporalBin outputBin);

    void aggregateTemporalBin(TemporalBin inputBin, TemporalBin outputBin);

    void completeTemporalBin(TemporalBin bin);

    void computeOutput(TemporalBin temporalBin, WritableVector outputVector);

}
