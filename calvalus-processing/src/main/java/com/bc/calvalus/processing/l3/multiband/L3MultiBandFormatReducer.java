package com.bc.calvalus.processing.l3.multiband;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TemporalBinSource;

import java.io.IOException;
import java.util.Iterator;

/**
 * The reducer for for formatting
 * multiple regions of a Binning product at once.
 */
public class L3MultiBandFormatReducer extends Reducer<L3MultiRegionBinIndex, FloatWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        TemporalBinSource temporalBinSource = new ReduceTemporalBinSource(context);
        Configuration conf = context.getConfiguration();

        String[] featureNames = conf.getStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES);
        String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");
        int partition = context.getTaskAttemptID().getTaskID().getId();
        String bandName = featureNames[partition];
        String productName = String.format("%s_%s_%s_%s", outputPrefix, bandName, dateStart, dateStop);
        conf.set(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, bandName);


        L3Formatter.write(context, temporalBinSource,
                          dateStart, dateStop,
                          null, null,
                          productName);
    }

    private class ReduceTemporalBinSource implements TemporalBinSource {

        private final Context context;

        public ReduceTemporalBinSource(Context context) throws IOException, InterruptedException {
            this.context = context;
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<TemporalBin> getPart(int index) throws IOException {
            return new CloningIterator(context);
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static class CloningIterator implements Iterator<TemporalBin> {

        private final Context context;

        public CloningIterator(Context context) {
                    this.context = context;
                }

        @Override
        public boolean hasNext() {
            try {
                return context.nextKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public TemporalBin next() {
            try {
                L3MultiRegionBinIndex binIndex = context.getCurrentKey();
                final float value = context.getValues().iterator().next().get();

                TemporalBin clonedBin = new TemporalBin(binIndex.getBinIndex(), 1);
                clonedBin.setNumObs(0);
                clonedBin.setNumPasses(0);
                clonedBin.getFeatureValues()[0] = value;
                return clonedBin;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
