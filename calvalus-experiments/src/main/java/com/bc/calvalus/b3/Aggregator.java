package com.bc.calvalus.b3;

/**
 * @author Norman Fomferra
 */
public interface Aggregator {

    String getName();

    int getSpatialPropertyCount();

    String getSpatialPropertyName(int i);

    int getTemporalPropertyCount();

    String getTemporalPropertyName(int i);

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
     * @param spatialVector The spatial aggregation to complete.
     * @param numObs        The number of observations added so far.
     */
    void completeSpatial(WritableVector spatialVector, int numObs);

    /**
     * Aggregates a spatial aggregation to a temporal aggregation.
     *
     * @param spatialVector  The spatial aggregation.
     * @param numSpatialObs  The number of total observations made in the spatial aggregation.
     * @param temporalVector The temporal aggregation to be updated.
     */
    void aggregateTemporal(Vector spatialVector, int numSpatialObs, WritableVector temporalVector);
}
