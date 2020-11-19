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

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.commons.DateUtils;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.calvalus.processing.l2.L2FormattingMapper;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.Credentials;
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

import java.io.IOException;
import java.net.URI;
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
            final boolean generateEmptyAggregate = conf.getBoolean("calvalus.generateEmptyAggregate", false);
            String format = conf.get(JobConfigNames.CALVALUS_OUTPUT_FORMAT, null);
            if ((numReducers == 1 || "org.esa.snap.binning.support.IsinPlanetaryGrid".equals(binningConfig.getPlanetaryGrid())) && format != null) {
                // if only one reducer and output format parameter set, format directly

                // handle metadata
                // it is always the first key in some reducer
                // unless we had not inputs at all
                // TODO what happens if there is no metadata key or does this never happen ??? cell-l3-workflow !!
                final boolean lookingAtNext = context.nextKey();
                if (! lookingAtNext) {
                    if (generateEmptyAggregate) {
                        CalvalusLogger.getLogger().info("no contributions, generating empty output");
                        context = new WrappedContext(context, lookingAtNext);
                    } else {
                        return;
                    }
                } else if (context.getCurrentKey().get() == L3SpatialBin.METADATA_MAGIC_NUMBER) {
                    CalvalusLogger.getLogger().info("metadata record seen");
                    processingGraphMetadata = aggregateMetadata(context.getValues());
                    final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
                    conf.set(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);
                } else {
                    CalvalusLogger.getLogger().info("no metadata record seen, resetting iterator");
                    context = new WrappedContext(context, lookingAtNext);
                }

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

    class WrappedContext extends Context {
        final Context delegate;
        boolean lookingAtNext;
        public WrappedContext(Context delegate, boolean lookingAtNext) {
            this.delegate = delegate;
            this.lookingAtNext = lookingAtNext;
        }
        @Override
        public boolean nextKey() throws IOException, InterruptedException {
            if (lookingAtNext) {
                lookingAtNext = false;
                return true;
            }
            return delegate.nextKey();
        }

        @Override
        public Iterable<L3SpatialBin> getValues() throws IOException, InterruptedException {
            return delegate.getValues();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return delegate.nextKeyValue();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return delegate.getCurrentKey();
        }

        @Override
        public L3SpatialBin getCurrentValue() throws IOException, InterruptedException {
            return delegate.getCurrentValue();
        }

        @Override
        public void write(LongWritable key, L3TemporalBin value) throws IOException, InterruptedException {
            delegate.write(key, value);

        }

        @Override
        public OutputCommitter getOutputCommitter() {
            return delegate.getOutputCommitter();
        }

        @Override
        public TaskAttemptID getTaskAttemptID() {
            return delegate.getTaskAttemptID();
        }

        @Override
        public void setStatus(String msg) {
            delegate.setStatus(msg);
        }

        @Override
        public String getStatus() {
            return delegate.getStatus();
        }

        @Override
        public float getProgress() {
            return delegate.getProgress();
        }

        @Override
        public Counter getCounter(Enum<?> counterName) {
            return delegate.getCounter(counterName);
        }

        @Override
        public Counter getCounter(String groupName, String counterName) {
            return delegate.getCounter(groupName, counterName);
        }

        @Override
        public Configuration getConfiguration() {
            return delegate.getConfiguration();
        }

        @Override
        public Credentials getCredentials() {
            return delegate.getCredentials();
        }

        @Override
        public JobID getJobID() {
            return delegate.getJobID();
        }

        @Override
        public int getNumReduceTasks() {
            return delegate.getNumReduceTasks();
        }

        @Override
        public Path getWorkingDirectory() throws IOException {
            return delegate.getWorkingDirectory();
        }

        @Override
        public Class<?> getOutputKeyClass() {
            return delegate.getOutputKeyClass();
        }

        @Override
        public Class<?> getOutputValueClass() {
            return delegate.getOutputValueClass();
        }

        @Override
        public Class<?> getMapOutputKeyClass() {
            return delegate.getMapOutputKeyClass();
        }

        @Override
        public Class<?> getMapOutputValueClass() {
            return delegate.getMapOutputValueClass();
        }

        @Override
        public String getJobName() {
            return delegate.getJobName();
        }

        @Override
        public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
            return null;
        }

        @Override
        public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
            return delegate.getMapperClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
            return delegate.getCombinerClass();
        }

        @Override
        public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
            return delegate.getReducerClass();
        }

        @Override
        public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
            return delegate.getOutputFormatClass();
        }

        @Override
        public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
            return delegate.getPartitionerClass();
        }

        @Override
        public RawComparator<?> getSortComparator() {
            return delegate.getSortComparator();
        }

        @Override
        public String getJar() {
            return delegate.getJar();
        }

        @Override
        public RawComparator<?> getCombinerKeyGroupingComparator() {
            return delegate.getCombinerKeyGroupingComparator();
        }

        @Override
        public RawComparator<?> getGroupingComparator() {
            return delegate.getGroupingComparator();
        }

        @Override
        public boolean getJobSetupCleanupNeeded() {
            return delegate.getJobSetupCleanupNeeded();
        }

        @Override
        public boolean getTaskCleanupNeeded() {
            return delegate.getTaskCleanupNeeded();
        }

        @Override
        public boolean getProfileEnabled() {
            return delegate.getProfileEnabled();
        }

        @Override
        public String getProfileParams() {
            return delegate.getProfileParams();
        }

        @Override
        public Configuration.IntegerRanges getProfileTaskRange(boolean isMap) {
            return delegate.getProfileTaskRange(isMap);
        }

        @Override
        public String getUser() {
            return delegate.getUser();
        }

        @Override
        public boolean getSymlink() {
            return delegate.getSymlink();
        }

        @Override
        public Path[] getArchiveClassPaths() {
            return delegate.getArchiveClassPaths();
        }

        @Override
        public URI[] getCacheArchives() throws IOException {
            return delegate.getCacheArchives();
        }

        @Override
        public URI[] getCacheFiles() throws IOException {
            return delegate.getCacheFiles();
        }

        @Override
        public Path[] getLocalCacheArchives() throws IOException {
            return delegate.getLocalCacheArchives();
        }

        @Override
        public Path[] getLocalCacheFiles() throws IOException {
            return delegate.getLocalCacheFiles();
        }

        @Override
        public Path[] getFileClassPaths() {
            return delegate.getFileClassPaths();
        }

        @Override
        public String[] getArchiveTimestamps() {
            return delegate.getArchiveTimestamps();
        }

        @Override
        public String[] getFileTimestamps() {
            return delegate.getFileTimestamps();
        }

        @Override
        public int getMaxMapAttempts() {
            return delegate.getMaxMapAttempts();
        }

        @Override
        public int getMaxReduceAttempts() {
            return delegate.getMaxReduceAttempts();
        }

        @Override
        public void progress() {
            delegate.progress();
        }
    }

}
