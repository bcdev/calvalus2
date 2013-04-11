package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l2.ProductFormatter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TemporalBinSource;
import org.esa.beam.binning.operator.FormatterConfig;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  The reducer for for formatting
 *  multiple regions of a Binning product at once.
 */
public class L3MultiRegionFormatReducer extends Reducer<L3RegionBinIndex, L3TemporalBin, NullWritable, NullWritable> implements Configurable {
    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();

    private Configuration conf;

    private TemporalBin value;
    private int currentRegion;
    private L3MultiRegionFormatConfig.Region region;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        while (readNext(context)) {
            writeProduct(context, new ReduceTemporalBinSource(context));
        }
    }

    public boolean readNext(Context context) throws IOException, InterruptedException {
        boolean hasNext = context.nextKey();
        if (hasNext) {
            value = context.getCurrentValue();
            L3RegionBinIndex currentKey = context.getCurrentKey();
            value.setIndex(currentKey.getBinIndex());
            if (currentRegion == -1) {
                currentRegion = currentKey.getRegionIndex();
                L3MultiRegionFormatConfig l3MultiRegionFormatConfig = L3MultiRegionFormatConfig.get(conf);
                region = l3MultiRegionFormatConfig.getRegions()[currentRegion];
            }
            return true;
        } else {
            value = null;
            return false;
        }
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
        public Iterator<? extends TemporalBin> getPart(int index) throws IOException {
            return new Iterator<TemporalBin>() {

                @Override
                public boolean hasNext() {
                    return value != null;
                }

                @Override
                public TemporalBin next() {
                    TemporalBin currentValue = value;
                    try {
                        readNext(context);
                    } catch (IOException ie) {
                        throw new RuntimeException("next value iterator failed", ie);
                    } catch (InterruptedException ie) {
                        // this is bad, but we can't modify the exception list of java.util
                        throw new RuntimeException("next value iterator interrupted", ie);
                    }
                    return currentValue;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("remove not implemented");
                }
            };
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private void writeProduct(Context context, TemporalBinSource temporalBinSource) throws IOException {
        String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
        String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
        String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");

        // todo - specify common Calvalus L3 productName convention (mz)
        String productName = String.format("%s_%s_%s_%s", outputPrefix, region.getName(), dateStart, dateStop);

        String format = conf.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
        String compression = conf.get(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, null);
        ProductFormatter productFormatter = new ProductFormatter(productName, format, compression);
        String outputFormat = productFormatter.getOutputFormat();
        try {
            File productFile = productFormatter.createTemporaryProductFile();

            // todo - make 'outputType' a production request parameter (mz)
            String outputType = "Product";
            // todo - make 'bandConfiguration' a production request parameter (mz)
            FormatterConfig.BandConfiguration[] rgbBandConfig = new FormatterConfig.BandConfiguration[0];

            L3FormatterConfig formatterConfig = new L3FormatterConfig(outputType,
                    productFile.getAbsolutePath(),
                    outputFormat,
                    rgbBandConfig);

            L3Config l3Config = L3Config.get(conf);
            L3Formatter formatter = new L3Formatter(l3Config.createBinningContext(),
                    L3FormatterConfig.parseTime(dateStart),
                    L3FormatterConfig.parseTime(dateStop),
                    conf);

            LOG.info("Start formatting product to file: " + productFile.getName());
            context.setStatus("formatting");
            formatter.format(temporalBinSource,
                    formatterConfig,
                    region.getName(),
                    region.getRegionWKT());

            LOG.info("Finished formatting product.");
            context.setStatus("copying");
            productFormatter.compressToHDFS(context, productFile);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Product formatted").increment(1);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Formatting failed.", e);
            throw new IOException(e);
        } finally {
            productFormatter.cleanupTempDir();
            context.setStatus("");
        }
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
