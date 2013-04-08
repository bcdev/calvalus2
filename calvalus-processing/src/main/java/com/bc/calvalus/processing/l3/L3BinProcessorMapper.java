package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.binprocessing.BinProcessor;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.Observation;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.support.ObservationImpl;
import org.esa.beam.binning.support.VectorImpl;

import java.io.IOException;

/**
 * A mapper to follow on with temporal bins....
 */
public class L3BinProcessorMapper extends Mapper<LongWritable, L3TemporalBin, LongWritable, L3SpatialBin> implements Configurable {

    private Configuration conf;
    private BinProcessor binProcessor = null;
    private BinManager firstBinManager;
    private BinManager secondBinManager;
    private float[] outputFeatures;
    private VectorImpl outputVector;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        firstBinManager.computeOutput(temporalBin, outputVector);
        final float[] values;
        if (binProcessor != null) {
            values = binProcessor.process(outputFeatures);
        } else {
            values = outputFeatures;
        }
        Observation observation = new ObservationImpl(0.0, 0.0, 0.0, values);
        SpatialBin spatialBin = secondBinManager.createSpatialBin(binIndex.get());
        secondBinManager.aggregateSpatialBin(observation, spatialBin);
        secondBinManager.completeSpatialBin(spatialBin);
        context.write(binIndex, (L3SpatialBin) spatialBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        firstBinManager = getFirstL3Config(conf).createBinningContext().getBinManager();
        secondBinManager = L3Config.get(conf).createBinningContext().getBinManager();
        outputFeatures = new float[firstBinManager.getOutputFeatureNames().length];
        outputVector = new VectorImpl(outputFeatures);

        // TODO example for bin processing
        // TODO use registry or similar construct for creation
//        BandShiftBinProcessor.BandShiftBinProcessorDescriptor processorDescriptor = new BandShiftBinProcessor.BandShiftBinProcessorDescriptor();
//        BinProcessorConfig config = processorDescriptor.createBinProcessorConfig();
//        binProcessor = processorDescriptor.createBinProcessor(secondBinManager.getVariableContext(), config);
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
