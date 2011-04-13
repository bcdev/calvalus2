package com.bc.calvalus.binning;

/**
 * todo - add api doc
 *
 * @author Norman Fomferra
 */
public class BinManagerImpl implements BinManager {
    private final Aggregator[] aggregators;
    private final int spatialPropertyCount;
    private final int temporalPropertyCount;
    private final int outputPropertyCount;
    private final int[] spatialPropertyOffsets;
    private final int[] temporalPropertyOffsets;
    private final int[] outputPropertyOffsets;
    private final String[] outputPropertyNames;
    private final double[] outputPropertyFillValues;


    public BinManagerImpl(Aggregator... aggregators) {
        this.aggregators = aggregators.clone();
        this.spatialPropertyOffsets = new int[aggregators.length];
        this.temporalPropertyOffsets = new int[aggregators.length];
        this.outputPropertyOffsets = new int[aggregators.length];
        int spatialPropertyCount = 0;
        int temporalPropertyCount = 0;
        int outputPropertyCount = 0;
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            spatialPropertyOffsets[i] = spatialPropertyCount;
            temporalPropertyOffsets[i] = temporalPropertyCount;
            outputPropertyOffsets[i] = outputPropertyCount;
            spatialPropertyCount += aggregator.getSpatialPropertyCount();
            temporalPropertyCount += aggregator.getTemporalPropertyCount();
            outputPropertyCount += aggregator.getOutputPropertyCount();
        }
        this.spatialPropertyCount = spatialPropertyCount;
        this.temporalPropertyCount = temporalPropertyCount;
        this.outputPropertyCount = outputPropertyCount;
        this.outputPropertyNames = new String[outputPropertyCount];
        this.outputPropertyFillValues = new double[outputPropertyCount];
        for (int i = 0, k = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            for (int j = 0; j < aggregator.getOutputPropertyCount(); j++) {
                outputPropertyNames[k] = aggregator.getOutputPropertyName(j);
                outputPropertyFillValues[k] = aggregator.getOutputPropertyFillValue(j);
                k++;
            }
        }
    }

    @Override
    public int getOutputPropertyCount() {
        return outputPropertyCount;
    }

    @Override
    public String getOutputPropertyName(int i) {
        return outputPropertyNames[i];
    }

    @Override
    public double getOutputPropertyFillValue(int i) {
        return outputPropertyFillValues[i];
    }

    @Override
    public int getAggregatorCount() {
        return aggregators.length;
    }

    @Override
    public Aggregator getAggregator(int aggIndex) {
        return aggregators[aggIndex];
    }

    @Override
    public Vector getSpatialVector(SpatialBin bin, int aggIndex) {
        final VectorImpl vector = new VectorImpl(bin.properties);
        final Aggregator aggregator = aggregators[aggIndex];
        vector.setOffsetAndSize(spatialPropertyOffsets[aggIndex], aggregator.getSpatialPropertyCount());
        return vector;
    }

    @Override
    public Vector getTemporalVector(TemporalBin bin, int aggIndex) {
        final VectorImpl vector = new VectorImpl(bin.properties);
        final Aggregator aggregator = aggregators[aggIndex];
        vector.setOffsetAndSize(temporalPropertyOffsets[aggIndex], aggregator.getTemporalPropertyCount());
        return vector;
    }

    @Override
    public WritableVector createOutputVector() {
        return new VectorImpl(new float[outputPropertyCount]);
    }

    @Override
    public SpatialBin createSpatialBin(long binIndex) {
        final SpatialBin spatialBin = new SpatialBin(binIndex, spatialPropertyCount);
        initSpatialBin(spatialBin);
        return spatialBin;
    }

     @Override
    public void aggregateSpatialBin(Observation observation, SpatialBin spatialBin) {
        final VectorImpl spatialVector = new VectorImpl(spatialBin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialPropertyOffsets[i], aggregator.getSpatialPropertyCount());
            aggregator.aggregateSpatial(spatialBin, observation, spatialVector);
        }
        spatialBin.numObs++;
    }

    @Override
    public void completeSpatialBin(SpatialBin spatialBin) {
        final VectorImpl spatialVector = new VectorImpl(spatialBin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialPropertyOffsets[i], aggregator.getSpatialPropertyCount());
            aggregator.completeSpatial(spatialBin, spatialBin.numObs, spatialVector);
        }
    }

    @Override
     public TemporalBin createTemporalBin(long binIndex) {
         final TemporalBin temporalBin = new TemporalBin(binIndex, temporalPropertyCount);
         initTemporalBin(temporalBin);
         return temporalBin;
     }

    @Override
    public void aggregateTemporalBin(SpatialBin spatialBin, TemporalBin temporalBin) {
        final VectorImpl spatialVector = new VectorImpl(spatialBin.properties);
        final VectorImpl temporalVector = new VectorImpl(temporalBin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            spatialVector.setOffsetAndSize(spatialPropertyOffsets[i], aggregator.getSpatialPropertyCount());
            temporalVector.setOffsetAndSize(temporalPropertyOffsets[i], aggregator.getTemporalPropertyCount());
            aggregator.aggregateTemporal(temporalBin, spatialVector, spatialBin.numObs, temporalVector);
        }
        temporalBin.numObs += spatialBin.numObs;
        temporalBin.numPasses++;
    }

    @Override
    public void completeTemporalBin(TemporalBin temporalBin) {
        final VectorImpl temporalVector = new VectorImpl(temporalBin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            temporalVector.setOffsetAndSize(spatialPropertyOffsets[i], aggregator.getSpatialPropertyCount());
            aggregator.completeTemporal(temporalBin, temporalBin.numObs, temporalVector);
        }
    }

    @Override
    public void computeOutput(TemporalBin temporalBin, WritableVector outputVector) {
        final VectorImpl temporalVector = new VectorImpl(temporalBin.properties);
        final VectorImpl outputVectorImpl = (VectorImpl) outputVector;
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            temporalVector.setOffsetAndSize(temporalPropertyOffsets[i], aggregator.getTemporalPropertyCount());
            outputVectorImpl.setOffsetAndSize(outputPropertyOffsets[i], aggregator.getOutputPropertyCount());
            aggregator.computeOutput(temporalVector, outputVector);
        }
        outputVectorImpl.setOffsetAndSize(0, outputPropertyCount);
    }

    private void initSpatialBin(SpatialBin bin) {
        final VectorImpl vector = new VectorImpl(bin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            vector.setOffsetAndSize(spatialPropertyOffsets[i], aggregator.getSpatialPropertyCount());
            aggregator.initSpatial(bin, vector);
        }
    }

    private void initTemporalBin(TemporalBin bin) {
        final VectorImpl vector = new VectorImpl(bin.properties);
        for (int i = 0; i < aggregators.length; i++) {
            final Aggregator aggregator = aggregators[i];
            vector.setOffsetAndSize(temporalPropertyOffsets[i], aggregator.getTemporalPropertyCount());
            aggregator.initTemporal(bin, vector);
        }
    }
}
