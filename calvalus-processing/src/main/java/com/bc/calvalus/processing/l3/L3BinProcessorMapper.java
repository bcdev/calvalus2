package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.Observation;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.support.ObservationImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * A mapper to follow on with temporal bins....
 */
public class L3BinProcessorMapper extends Mapper<LongWritable, L3TemporalBin, LongWritable, L3SpatialBin> implements Configurable {

    private Configuration conf;

    private BinManager binManager;
    private float[] observationFeatures;
    private Observation observation;

    private int[] resultIndexes;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        final float[] temporalFeatures = temporalBin.getFeatureValues();

        // map features from result to observation vector
        for (int i = 0; i < resultIndexes.length; i++) {
            observationFeatures[i] = temporalFeatures[resultIndexes[i]];
        }

        // start 2nd L3 processing computeSpatial
        SpatialBin spatialBin = binManager.createSpatialBin(binIndex.get());
        binManager.aggregateSpatialBin(observation, spatialBin);
        binManager.completeSpatialBin(spatialBin);
        context.write(binIndex, (L3SpatialBin) spatialBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        binManager = L3Config.get(conf).createBinningContext().getBinManager();
        ArrayList<String> inputFeatureNames = new ArrayList<String>();
        Collections.addAll(inputFeatureNames, getInputFeatureNames(conf));
        VariableContext variableContext = binManager.getVariableContext();
        int variableCount = variableContext.getVariableCount();

        observationFeatures = new float[variableCount];
        observation = new ObservationImpl(0.0, 0.0, 0.0, observationFeatures);

        resultIndexes = new int[variableCount];
        for (int i = 0; i < resultIndexes.length; i++) {
            String obsName = variableContext.getVariableName(i);
            resultIndexes[i] = inputFeatureNames.indexOf(obsName);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    static String[] getInputFeatureNames(Configuration jobConfig) {
        String[] featureNames = jobConfig.getStrings("calvalus.l3.inputFeatureNames");
        if (featureNames != null) {
            return featureNames;
        }

        // legacy method could be removed later
        String xml = jobConfig.get(JobConfigNames.CALVALUS_L3_PARAMETERS_FIRST);
        if (xml == null) {
            throw new IllegalArgumentException("Missing (first) L3 configuration '" + JobConfigNames.CALVALUS_L3_PARAMETERS_FIRST + "'");
        }
        L3Config l3Config;
        try {
            l3Config = L3Config.fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid (first) L3 configuration: " + e.getMessage(), e);
        }
        BinManager binManager = l3Config.createBinningContext().getBinManager();
        return binManager.getResultFeatureNames();
    }
}
