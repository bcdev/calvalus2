package com.bc.calvalus.processing.l3.multiregion;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.L3Formatter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinSource;

import java.io.IOException;
import java.util.Iterator;

/**
 * The reducer for for formatting
 * multiple regions of a Binning product at once.
 */
public class L3MultiRegionFormatReducer extends Reducer<L3MultiRegionBinIndex, L3MultiRegionTemporalBin, NullWritable, NullWritable> implements Configurable {

    private Configuration conf;
    private L3MultiRegionFormatConfig l3MultiRegionFormatConfig;

    @Override
    protected void reduce(L3MultiRegionBinIndex key, Iterable<L3MultiRegionTemporalBin> values, Context context) throws IOException, InterruptedException {
        L3MultiRegionFormatConfig.Region region = l3MultiRegionFormatConfig.getRegions()[key.getRegionIndex()];
        writeProduct(context, region, new ReduceTemporalBinSource(values));
    }

    private class ReduceTemporalBinSource implements TemporalBinSource {

        private final Iterable<L3MultiRegionTemporalBin> values;

        public ReduceTemporalBinSource(Iterable<L3MultiRegionTemporalBin> values) throws IOException, InterruptedException {
            this.values = values;
        }

        @Override
        public int open() throws IOException {
            return 1;
        }

        @Override
        public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
            return new CloningIterator(values.iterator());
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private static class CloningIterator implements Iterator<TemporalBin> {

        Iterator<L3MultiRegionTemporalBin> delegate;

        private CloningIterator(Iterator<L3MultiRegionTemporalBin> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public TemporalBin next() {
            L3MultiRegionTemporalBin bin = delegate.next();
            float[] featureValues = bin.getFeatureValues();
            L3MultiRegionTemporalBin clonedBin = new L3MultiRegionTemporalBin(bin.getIndex(), featureValues.length);
            clonedBin.setNumObs(bin.getNumObs());
            clonedBin.setNumPasses(bin.getNumPasses());
            System.arraycopy(featureValues, 0, clonedBin.getFeatureValues(), 0, featureValues.length);
            return clonedBin;
        }

        @Override
        public void remove() {
        }
    }

    private void writeProduct(Context context, L3MultiRegionFormatConfig.Region region, TemporalBinSource temporalBinSource) throws IOException {
        String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");

        // todo - specify common Calvalus L3 productName convention (mz)
        String productName = String.format("%s_%s_%s_%s", outputPrefix, region.getName(), dateStart, dateStop);

        L3Formatter.write(context, temporalBinSource,
                          dateStart, dateStop,
                          region.getName(), region.getRegionWKT(),
                          productName);
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.l3MultiRegionFormatConfig = L3MultiRegionFormatConfig.get(conf);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }


}
