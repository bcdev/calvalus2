package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l3.cellstream.CellFileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.binning.BinManager;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.Observation;
import org.esa.beam.binning.SpatialBin;
import org.esa.beam.binning.VariableContext;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.support.ObservationImpl;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;

/**
 * A mapper to follow on with temporal bins....
 */
public class CellL3ProcessorMapper extends Mapper<LongWritable, L3TemporalBin, LongWritable, L3SpatialBin> {

    private static final DateFormat DATE_FORMAT = ProductData.UTC.createDateFormat("yyyy-MM-dd");

    private BinManager binManager;
    private float[] observationFeatures;
    private Observation observation;
    private int[] resultIndexes;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        CellFileSplit cellFileSplit = (CellFileSplit) context.getInputSplit();

        Map<String, String> metadata;
        if (cellFileSplit.hasMetadata()) {
            metadata = cellFileSplit.getMetadata();
        } else {
            Path inputDirectory = cellFileSplit.getPath().getParent();
            metadata = ProcessingMetadata.read(inputDirectory, conf);
        }
        ProcessingMetadata.metadata2Config(metadata, conf, JobConfigNames.LEVEL3_METADATA_KEYS);

        BinningContext binningContext = HadoopBinManager.createBinningContext(conf, null);
        binManager = binningContext.getBinManager();

        String[] featureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
        ArrayList<String> inputFeatureNames = new ArrayList<String>();
        Collections.addAll(inputFeatureNames, featureNames);
        VariableContext variableContext = binManager.getVariableContext();
        int variableCount = variableContext.getVariableCount();

        observationFeatures = new float[variableCount];
        observation = new ObservationImpl(0.0, 0.0, getMeanMJD(conf), observationFeatures);

        resultIndexes = new int[variableCount];
        for (int i = 0; i < resultIndexes.length; i++) {
            String obsName = variableContext.getVariableName(i);
            resultIndexes[i] = inputFeatureNames.indexOf(obsName);
        }
    }

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

    static double getMeanMJD(Configuration conf) {
        double min = getMJD(conf.get(JobConfigNames.CALVALUS_MIN_DATE));
        double max = getMJD(conf.get(JobConfigNames.CALVALUS_MAX_DATE));
        if (min == 0.0 || max == 0.0) {
            return 0.0;
        }
        // because out max date is inclusive, increase by one
        return ((max + 1.0 - min) / 2.0) + min;
    }

    static double getMJD(String dateString) {
        if (dateString == null) {
            return 0;
        }
        Date date;
        try {
            date = DATE_FORMAT.parse(dateString);
        } catch (ParseException e) {
            CalvalusLogger.getLogger().log(Level.WARNING, "failed to parse date. " + e.getMessage(), e);
            return 0;
        }
        return ProductData.UTC.create(date, 0).getMJD();
    }
}
