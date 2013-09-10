package com.bc.calvalus.processing.l3;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.CellProcessor;
import org.esa.beam.binning.CellProcessorConfig;
import org.esa.beam.binning.CellProcessorDescriptor;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TypedDescriptorsRegistry;
import org.esa.beam.binning.WritableVector;
import org.esa.beam.binning.support.VariableContextImpl;

import java.io.IOException;

/**
 * A mapper to follow on with temporal bins....
 */
public class CellProcessorMapper extends Mapper<LongWritable, L3TemporalBin, LongWritable, L3TemporalBin> implements Configurable {

    private Configuration conf;

    private CellProcessor cellProcessor;
    private BinManager binManager;

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        TemporalBin processedBin = process(binIndex.get(), temporalBin);
        context.write(binIndex, (L3TemporalBin) processedBin);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        L3Config l3Config = L3Config.get(conf);
        binManager = l3Config.createBinningContext().getBinManager();

        CellProcessorConfig postProcessorConfig = l3Config.getBinningConfig().getPostProcessorConfig();
        String[] inputFeatureNames = conf.getStrings("calvalus.l3.inputFeatureNames");
        cellProcessor = createCellProcessor(postProcessorConfig, inputFeatureNames);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    // taken from BinningManger
    private static CellProcessor createCellProcessor(CellProcessorConfig config, String[] outputFeatureNames) {
        VariableContextImpl variableContextAgg = new VariableContextImpl();
        for (String outputFeatureName : outputFeatureNames) {
            variableContextAgg.defineVariable(outputFeatureName);
        }
        TypedDescriptorsRegistry registry = TypedDescriptorsRegistry.getInstance();
        CellProcessorDescriptor descriptor = registry.getDescriptor(CellProcessorDescriptor.class, config.getName());
        if (descriptor != null) {
            return descriptor.createCellProcessor(variableContextAgg, config);
        } else {
            throw new IllegalArgumentException("Unknown cell processor type: " + config.getName());
        }
    }

    // taken from CellProcessorChain
    private TemporalBin process(long binIndex, TemporalBin temporalBin) {
        WritableVector temporalVector = temporalBin.toVector();
        TemporalBin processBin = binManager.createProcessBin(binIndex);
        cellProcessor.compute(temporalVector, processBin.toVector());

        // will be removed soon TODO
        processBin.setNumObs(temporalBin.getNumObs());
        processBin.setNumPasses(temporalBin.getNumPasses());
        return processBin;
    }

}
