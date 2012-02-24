package com.bc.calvalus.binning;

/**
 * Abstract base class that provides the aggregator's static metadata.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class AbstractAggregator implements Aggregator {

    private final String name;
    private final String[] spatialFeatureNames;
    private final String[] temporalFeatureNames;
    private final String[] outputFeatureNames;
    private final float fillValue;

    protected AbstractAggregator(String name, String[] featureNames, Float fillValue) {
        this(name, featureNames, featureNames, featureNames, fillValue);
    }

    protected AbstractAggregator(String name,
                                 String[] spatialFeatureNames,
                                 String[] temporalFeatureNames,
                                 String[] outputFeatureNames,
                                 Float fillValue) {
        this.name = name;
        this.spatialFeatureNames = spatialFeatureNames;
        this.temporalFeatureNames = temporalFeatureNames;
        this.outputFeatureNames = outputFeatureNames;
        this.fillValue = fillValue != null ? fillValue : Float.NaN;
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public String[] getSpatialFeatureNames() {
        return spatialFeatureNames;
    }

    @Override
    public String[] getTemporalFeatureNames() {
        return temporalFeatureNames;
    }

    @Override
    public String[] getOutputFeatureNames() {
        return outputFeatureNames;
    }

    @Override
    public float getOutputFillValue() {
        return fillValue;
    }

    public static String[] createFeatureNames(String varName, String... names) {
        String[] featureNames = new String[names.length];
        for (int i = 0; i < names.length; i++) {
            featureNames[i] = varName + "_" + names[i];
        }
        return featureNames;
    }

}
