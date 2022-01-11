/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.processing.l3;

import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l2.L2FormattingMapper;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinSource;
import org.esa.snap.binning.TemporalBinner;
import org.esa.snap.binning.cellprocessor.CellProcessorChain;
import org.esa.snap.binning.operator.BinningConfig;
import org.esa.snap.binning.operator.metadata.GlobalMetadata;
import org.esa.snap.binning.operator.metadata.MetadataAggregator;
import org.esa.snap.binning.operator.metadata.MetadataAggregatorFactory;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;
import org.locationtech.jts.geom.Geometry;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Reducer extends Reducer<LongWritable, L3SpatialBin, LongWritable, L3TemporalBin> {

    private static final String DATETIME_OUTPUT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateFormat DATETIME_OUTPUT_FORMAT = DateUtils.createDateFormat(DATETIME_OUTPUT_PATTERN);

    static {
        DATETIME_OUTPUT_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private CellProcessorChain cellChain;
    private boolean computeOutput;
    private BinningConfig binningConfig;
    private MetadataElement processingGraphMetadata;
    private MetadataSerializer metadataSerializer;

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        try {
            int numReducers = conf.getInt(JobContext.NUM_REDUCES, 8);
            String format = conf.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
            if (numReducers == 1 && format != null) {
                // if only one reducer and output format parameter set, format directly

                // handle metadata
                // it is always the first key
                // TODO what happens if there is no metadata key or does this never happen ??? cell-l3-workflow !!
                context.nextKey();
                processingGraphMetadata = aggregateMetadata(context.getValues());
                final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
                conf.set(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);

                final TemporalBinSource temporalBinSource = new ReduceTemporalBinSource(context);

                String dateStart = conf.get(JobConfigNames.CALVALUS_MIN_DATE);
                String dateStop = conf.get(JobConfigNames.CALVALUS_MAX_DATE);
                String outputPrefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX, "L3");
                String regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
                String regionWKT = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);

                // todo - specify common Calvalus L3 productName convention (mz)
                String productName = String.format("%s_%s_%s", outputPrefix, dateStart, dateStop);
                if (context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REGEX) != null
                        && context.getConfiguration().get(JobConfigNames.CALVALUS_OUTPUT_REPLACEMENT) != null) {
                    productName = L2FormattingMapper.getProductName(context.getConfiguration(), productName);
                }

                L3Formatter.write(context, temporalBinSource,
                                  dateStart, dateStop,
                                  regionName, regionWKT,
                                  productName);
            } else {
                while (context.nextKey()) {
                    reduce(context.getCurrentKey(), context.getValues(), context);
                }
            }
        } finally {
            cleanup(context);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        computeOutput = conf.getBoolean(JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, true);
        binningConfig = getL3Config(conf);
        metadataSerializer = new MetadataSerializer();

        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, null, regionGeometry);
        temporalBinner = new TemporalBinner(binningContext);
        cellChain = new CellProcessorChain(binningContext);
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, binningContext.getBinManager().getResultFeatureNames());
    }

    private static BinningConfig getL3Config(Configuration conf) {
        String cellL3Conf = conf.get(JobConfigNames.CALVALUS_CELL_PARAMETERS);
        String stdL3Conf = conf.get(JobConfigNames.CALVALUS_L3_PARAMETERS);
        String l3ConfXML;
        if (cellL3Conf != null) {
            l3ConfXML = cellL3Conf;
        } else {
            l3ConfXML = stdL3Conf;
        }
        try {
            return BinningConfig.fromXml(l3ConfXML);
        } catch (BindingException e) {
            throw new IllegalArgumentException("Invalid L3 configuration: " + e.getMessage(), e);
        }
    }

    @Override
    protected void reduce(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        TemporalBin temporalBin = aggregate(binIndex, spatialBins);
        if (temporalBin != null) {
            context.write(binIndex, (L3TemporalBin) temporalBin);
        }
    }

    private TemporalBin aggregate(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins) throws IOException, InterruptedException {
        final long idx = binIndex.get();
        if (idx == L3SpatialBin.METADATA_MAGIC_NUMBER) {
            processingGraphMetadata = aggregateMetadata(spatialBins);
            return null;
        } else {
            TemporalBin temporalBin = temporalBinner.processSpatialBins(idx, spatialBins);

            if (computeOutput) {
                temporalBin = temporalBinner.computeOutput(idx, temporalBin);
                temporalBin = cellChain.process(temporalBin);
            }
            return temporalBin;
        }
    }

    private MetadataElement aggregateMetadata(Iterable<L3SpatialBin> spatialBins) {
        String metadataAggregatorName = binningConfig.getMetadataAggregatorName();
        final MetadataAggregator metadataAggregator = MetadataAggregatorFactory.create(metadataAggregatorName);
        for (L3SpatialBin metadataBin : spatialBins) {
            final String metadataXml = metadataBin.getMetadata();
            final MetadataElement metadataElement = metadataSerializer.fromXml(metadataXml);
            metadataAggregator.aggregateMetadata(metadataElement);
        }
        MetadataElement sourcesMetadata = metadataAggregator.getMetadata();
        return createL3Metadata(sourcesMetadata, binningConfig, conf);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // only write this file in the first reducer
        final int partition = context.getTaskAttemptID().getTaskID().getId();
        if (partition == 0) {
            final Map<String, String> metadata = ProcessingMetadata.config2metadata(conf, JobConfigNames.LEVEL3_METADATA_KEYS);
            final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
            metadata.put(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);
            final Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            ProcessingMetadata.write(workOutputPath, conf, metadata);
        }
    }

    private static MetadataElement createL3Metadata(MetadataElement sourcesMetadata, BinningConfig binningConfig, Configuration conf) {
        final GlobalMetadata globalMetadata = GlobalMetadata.create(binningConfig);
        final MetadataElement processingGraphMetadata = globalMetadata.asMetadataElement();
        final MetadataElement node_0 = processingGraphMetadata.getElement("node.0");
        final MetadataElement parameters = node_0.getElement("parameters");

        addCalvalusMetadata(node_0, conf);
        addCalvalusParameters(parameters, conf);
        node_0.addElement(sourcesMetadata);

        return processingGraphMetadata;
    }

    private static void addCalvalusMetadata(MetadataElement element, Configuration conf) {
        addAttributeToMetadataElement(element, "operator", conf.get(JobConfigNames.CALVALUS_PRODUCTION_TYPE));
        addAttributeToMetadataElement(element, "calvalusVersion", conf.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE));
        addAttributeToMetadataElement(element, "snapVersion", conf.get(JobConfigNames.CALVALUS_SNAP_BUNDLE));
        addAttributeToMetadataElement(element, "user", conf.get(JobConfigNames.CALVALUS_USER));
    }

    private static void addCalvalusParameters(MetadataElement element, Configuration conf) {
        addAttributeToMetadataElement(element, "aggregation_period_start", conf.get(JobConfigNames.CALVALUS_MIN_DATE));
        addAttributeToMetadataElement(element, "aggregation_period_end", conf.get(JobConfigNames.CALVALUS_MAX_DATE));
        addAttributeToMetadataElement(element, "region_name", conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME));
        addAttributeToMetadataElement(element, "region", conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
    }

    private static void addAttributeToMetadataElement(MetadataElement parent, String name, String value) {
        if (value != null) {
            parent.addAttribute(new MetadataAttribute(name, ProductData.createInstance(value), true));
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
            return new ReducingIterator(context);
        }

        @Override
        public void partProcessed(int index, Iterator<? extends TemporalBin> part) throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    private class ReducingIterator implements Iterator<TemporalBin> {

        private final Context context;

        public ReducingIterator(Context context) {
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
                LongWritable binIndex = context.getCurrentKey();
                Iterable<L3SpatialBin> spatialBins = context.getValues();
                TemporalBin temporalBin = aggregate(binIndex, spatialBins);
                context.write(binIndex, (L3TemporalBin) temporalBin);
                return temporalBin;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
