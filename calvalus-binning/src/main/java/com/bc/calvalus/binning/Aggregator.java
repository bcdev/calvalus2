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

    double getOutputPropertyFillValue(int i);

    /**
     * Initialises the spatial aggregation vector.
     *
     * @param ctx    The bin context which is shared between calls to {@link #initSpatial},
     *               {@link #aggregateSpatial} and {@link #completeSpatial}.
     * @param vector The aggregation vector to initialise.
     */
    void initSpatial(BinContext ctx, WritableVector vector);

    /**
     * Aggregates a new observation to a spatial aggregation vector.
     *
     * @param ctx               The bin context which is shared between calls to {@link #initSpatial},
     *                          {@link #aggregateSpatial} and {@link #completeSpatial}.
     * @param observationVector The observation.
     * @param spatialVector     The spatial aggregation vector to update.
     */
    void aggregateSpatial(BinContext ctx, Vector observationVector, WritableVector spatialVector);

    /**
     * Informs this aggregation instance that no more measurements will be added to the spatial vector.
     *
     * @param ctx           The bin context which is shared between calls to {@link #initSpatial},
     *                      {@link #aggregateSpatial} and {@link #completeSpatial}.
     * @param numSpatialObs The number of observations added so far.
     * @param spatialVector The spatial aggregation vector to complete.
     */
    void completeSpatial(BinContext ctx, int numSpatialObs, WritableVector spatialVector);

    /**
     * Initialises the temporal aggregation vector.
     *
     * @param ctx    The bin context which is shared between calls to {@link #initTemporal},
     *               {@link #aggregateTemporal} and {@link #completeTemporal}.
     * @param vector The aggregation vector to initialise.
     */
    void initTemporal(BinContext ctx, WritableVector vector);

    /**
     * Aggregates a spatial aggregation to a temporal aggregation vector.
     *
     * @param ctx            The bin context which is shared between calls to {@link #initTemporal},
     *                       {@link #aggregateTemporal} and {@link #completeTemporal}.
     * @param spatialVector  The spatial aggregation.
     * @param numSpatialObs  The number of total observations made in the spatial aggregation.
     * @param temporalVector The temporal aggregation vector to be updated.
     */
    void aggregateTemporal(BinContext ctx, Vector spatialVector, int numSpatialObs, WritableVector temporalVector);

    /**
     * Informs this aggregation instance that no more measurements will be added to the temporal vector.
     *
     * @param ctx            The bin context which is shared between calls to {@link #initTemporal},
     *                       {@link #aggregateTemporal} and {@link #completeTemporal}.
     * @param numTemporalObs The number of observations added so far.
     * @param temporalVector The temporal aggregation vector to complete.
     */
    void completeTemporal(BinContext ctx, int numTemporalObs, WritableVector temporalVector);

    /**
     * Computes the output vector from the temporal vector.
     *
     * @param temporalVector The temporal vector.
     * @param outputVector   The output vector to be computed.
     */
    void computeOutput(Vector temporalVector, WritableVector outputVector);

}
