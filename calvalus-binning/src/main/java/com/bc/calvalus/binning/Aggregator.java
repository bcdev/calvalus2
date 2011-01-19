package com.bc.calvalus.binning;

/**
 * @author Norman Fomferra
 */
public interface Aggregator {

    String getName();

    // todo - replace next 2 by PropertyContext getSpatialPropertyContext()

    int getSpatialPropertyCount();

    String getSpatialPropertyName(int i);

    // todo - replace next 2 by PropertyContext getTemporalPropertyContext()

    int getTemporalPropertyCount();

    String getTemporalPropertyName(int i);

    // todo - replace next 2 by PropertyContext getOutputPropertyContext()

    int getOutputPropertyCount();

    String getOutputPropertyName(int i);

    /**
     * Initialises the spatial aggregation.
     *
     * @param vector The aggregation to initialise.
     */
    void initSpatial(WritableVector vector);

    /**
     * Initialises the spatial aggregation.
     *
     * @param vector The aggregation to initialise.
     */
    void initTemporal(WritableVector vector);

    /**
     * Aggregates a new observation to a spatial aggregation.
     *
     * @param observationVector The observation.
     * @param spatialVector     The spatial aggregation to update.
     */
    void aggregateSpatial(Vector observationVector, WritableVector spatialVector);

    /**
     * Informs this aggregation instance that no more measurement will be added.
     *
     * @param numSpatialObs The number of observations added so far.
     * @param spatialVector The spatial aggregation to complete.
     */
    void completeSpatial(int numSpatialObs, WritableVector spatialVector);

    /**
     * Aggregates a spatial aggregation to a temporal aggregation.
     *
     * @param spatialVector  The spatial aggregation.
     * @param numSpatialObs  The number of total observations made in the spatial aggregation.
     * @param temporalVector The temporal aggregation to be updated.
     */
    void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector);

    /**
     * Computes the output vector from the temporal vector.
     *
     * @param temporalVector The temporal vector.
     * @param outputVector   The output vector to be computed.
     */
    void computeOutput(Vector temporalVector, WritableVector outputVector);
}
