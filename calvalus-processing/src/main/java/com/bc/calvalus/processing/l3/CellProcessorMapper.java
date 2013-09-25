package com.bc.calvalus.processing.l3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.binning.CellProcessor;
import org.esa.beam.binning.CellProcessorConfig;
import org.esa.beam.binning.CellProcessorDescriptor;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TypedDescriptorsRegistry;
import org.esa.beam.binning.WritableVector;
import org.esa.beam.binning.support.VariableContextImpl;

import java.io.IOException;
import java.util.Map;

/**
 * A mapper to follow on with temporal bins....
 */
public class CellProcessorMapper extends Mapper<LongWritable, L3TemporalBin, LongWritable, L3TemporalBin> {

    private CellProcessor cellProcessor;
    private int resultFeatureCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        L3Config l3Config = getCellL3Config(conf);
        CellProcessorConfig postProcessorConfig = l3Config.getBinningConfig().getPostProcessorConfig();

        String[] inputFeatureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
        cellProcessor = createCellProcessor(postProcessorConfig, inputFeatureNames);
        String[] outputFeatureNames = cellProcessor.getOutputFeatureNames();
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, outputFeatureNames);
        resultFeatureCount = outputFeatureNames.length;
    }

    @Override
    protected void map(LongWritable binIndex, L3TemporalBin temporalBin, Context context) throws IOException, InterruptedException {
        TemporalBin processedBin = process(binIndex.get(), temporalBin);
        context.write(binIndex, (L3TemporalBin) processedBin);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
        Map<String, String> metadata = ProcessingMetadata.config2metadata(conf, JobConfigNames.LEVEL3_METADATA_KEYS);
        ProcessingMetadata.write(workOutputPath, conf, metadata);
    }

    static L3Config getCellL3Config(Configuration jobConfig) {
        String xml = jobConfig.get(JobConfigNames.CALVALUS_CELL_PARAMETERS);
        if (xml == null) {
            throw new IllegalArgumentException("Missing Cell configuration '" + JobConfigNames.CALVALUS_CELL_PARAMETERS + "'");
        }
        try {
            return L3Config.fromXml(xml);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid Cell configuration: " + e.getMessage(), e);
        }
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

    private TemporalBin process(long binIndex, TemporalBin temporalBin) {
        WritableVector temporalVector = temporalBin.toVector();
        TemporalBin processBin = new L3TemporalBin(binIndex, resultFeatureCount);
        cellProcessor.compute(temporalVector, processBin.toVector());

        // will be removed soon TODO
        processBin.setNumObs(temporalBin.getNumObs());
        processBin.setNumPasses(temporalBin.getNumPasses());
        return processBin;
    }

}
