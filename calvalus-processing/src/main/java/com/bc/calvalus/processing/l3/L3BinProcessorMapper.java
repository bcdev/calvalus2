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

    private float[] resultFeatures;

    private BinManager secondBinManager;
    private float[] observationFeatures;
    private Observation observation;

    private int[] resultIndexes;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        // map features from result to observation vector
        for (int i = 0; i < resultIndexes.length; i++) {
            observationFeatures[i] = resultFeatures[resultIndexes[i]];
        }

        // start 2nd L3 processing computeSpatial
        SpatialBin spatialBin = secondBinManager.createSpatialBin(binIndex.get());
        secondBinManager.aggregateSpatialBin(observation, spatialBin);
        secondBinManager.completeSpatialBin(spatialBin);
        context.write(binIndex, (L3SpatialBin) spatialBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        BinManager firstBinManager = getFirstL3Config(conf).createBinningContext().getBinManager();
        secondBinManager = L3Config.get(conf).createBinningContext().getBinManager();
        ArrayList<String> resultNameList = new ArrayList<String>();
        Collections.addAll(resultNameList, firstBinManager.getResultFeatureNames());
        VariableContext variableContext = secondBinManager.getVariableContext();
        int variableCount = variableContext.getVariableCount();

        resultFeatures = new float[resultNameList.size()];

        observationFeatures = new float[variableCount];
        observation = new ObservationImpl(0.0, 0.0, 0.0, observationFeatures);

        resultIndexes = new int[variableCount];
        for (int i = 0; i < resultIndexes.length; i++) {
            String obsName = variableContext.getVariableName(i);
            resultIndexes[i] = resultNameList.indexOf(obsName);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    static L3Config getFirstL3Config(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_L3_PARAMETERS_FIRST);
        if (xml == null) {
            throw new IllegalArgumentException("Missing (first) L3 configuration '" + JobConfigNames.CALVALUS_L3_PARAMETERS_FIRST + "'");
        }
        try {
            return L3Config.fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid (first) L3 configuration: " + e.getMessage(), e);
        }
    }
}
