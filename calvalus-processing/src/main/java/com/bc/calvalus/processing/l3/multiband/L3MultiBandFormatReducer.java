package com.bc.calvalus.processing.l3.multiband;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Formatter;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionBinIndex;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionFormatConfig;
import com.bc.calvalus.processing.l3.multiregion.L3MultiRegionTemporalBin;
import org.apache.hadoop.conf.Configurable;
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
public class L3MultiBandFormatReducer extends Reducer<L3MultiRegionBinIndex, FloatWritable, NullWritable, NullWritable> implements Configurable {

    private Configuration conf;

    @Override
    protected void reduce(L3MultiRegionBinIndex key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        writeProduct(context, key.getRegionIndex(), new ReduceTemporalBinSource(key.getBinIndex(), values));
    }

    private class ReduceTemporalBinSource implements TemporalBinSource {

        private final long key;
        private final Iterable<FloatWritable> values;

        public ReduceTemporalBinSource(long key, Iterable<FloatWritable> values) throws IOException, InterruptedException {
            this.key = key;
            this.values = values;
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<TemporalBin> getPart(int index) throws IOException {
            return new CloningIterator(key, values.iterator());
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static class CloningIterator implements Iterator<TemporalBin> {

        private final long key;
        Iterator<FloatWritable> delegate;

        private CloningIterator(long key, Iterator<FloatWritable> delegate) {
            this.key = key;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public TemporalBin next() {
            FloatWritable bin = delegate.next();
            TemporalBin clonedBin = new TemporalBin(key, 1);
            clonedBin.setNumObs(0);
            clonedBin.setNumPasses(0);
            clonedBin.getFeatureValues()[0] = bin.get();
            return clonedBin;
        }

        @Override
        public void remove() {
        }
    }

    private void writeProduct(Context context, int bandIndex, TemporalBinSource temporalBinSource) throws IOException {
        String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");

        // todo - specify common Calvalus L3 productName convention (mz)
        String productName = String.format("%s_%s_%s_%s", outputPrefix, bandIndex, dateStart, dateStop);

        L3Formatter.write(context, temporalBinSource,
                          dateStart, dateStop,
                          null, null,
                          productName);
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


}
