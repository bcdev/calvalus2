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

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.hadoop.MetadataSerializer;
import com.bc.calvalus.processing.hadoop.ProcessingMetadata;
import com.bc.ceres.binding.BindingException;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.binning.BinningContext;
import org.esa.beam.binning.TemporalBin;
import org.esa.beam.binning.TemporalBinner;
import org.esa.beam.binning.cellprocessor.CellProcessorChain;
import org.esa.beam.binning.operator.BinningConfig;
import org.esa.beam.binning.operator.metadata.GlobalMetadata;
import org.esa.beam.binning.operator.metadata.MetadataAggregator;
import org.esa.beam.binning.operator.metadata.MetadataAggregatorFactory;
import org.esa.beam.framework.datamodel.MetadataAttribute;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.ProductData;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Reducer extends Reducer<LongWritable, L3SpatialBin, LongWritable, L3TemporalBin> implements Configurable {

    private static final String DATETIME_OUTPUT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final SimpleDateFormat DATETIME_OUTPUT_FORMAT = new SimpleDateFormat(DATETIME_OUTPUT_PATTERN);
    static {
        DATETIME_OUTPUT_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private CellProcessorChain cellChain;
    private boolean computeOutput;
    private MetadataAggregator metadataAggregator;
    private MetadataSerializer metadataSerializer;

    @Override
    protected void reduce(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        final long idx = binIndex.get();
        if (idx == L3SpatialBin.METADATA_MAGIC_NUMBER) {

            for (L3SpatialBin metadataBin : spatialBins) {
                final String metadataXml = metadataBin.getMetadata();
                final MetadataElement metadataElement = metadataSerializer.fromXml(metadataXml);
                metadataAggregator.aggregateMetadata(metadataElement);
            }
        } else {
            TemporalBin temporalBin = temporalBinner.processSpatialBins(idx, spatialBins);

            if (computeOutput) {
                temporalBin = temporalBinner.computeOutput(idx, temporalBin);
                temporalBin = cellChain.process(temporalBin);
            }
            context.write(binIndex, (L3TemporalBin) temporalBin);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // only write this file in the first reducer
        final int partition = context.getTaskAttemptID().getTaskID().getId();
        if (partition == 0) {
            final Map<String, String> metadata = ProcessingMetadata.config2metadata(conf, JobConfigNames.LEVEL3_METADATA_KEYS);
            final MetadataElement processingGraphMetadata = createL3Metadata();
            final String aggregatedMetadataXml = metadataSerializer.toXml(processingGraphMetadata);
            metadata.put(JobConfigNames.PROCESSING_HISTORY, aggregatedMetadataXml);
            final Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            ProcessingMetadata.write(workOutputPath, conf, metadata);
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        computeOutput = conf.getBoolean(JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, true);

        final BinningConfig binningConfig = getL3Config(conf);
        final String metadataAggregatorName = binningConfig.getMetadataAggregatorName();
        metadataAggregator = MetadataAggregatorFactory.create(metadataAggregatorName);
        metadataSerializer = new MetadataSerializer();

        Geometry regionGeometry = JobUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, null, regionGeometry);
        temporalBinner = new TemporalBinner(binningContext);
        cellChain = new CellProcessorChain(binningContext);
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, binningContext.getBinManager().getResultFeatureNames());
    }

    private MetadataElement createL3Metadata() {
        final String regionName = conf.get(JobConfigNames.CALVALUS_INPUT_REGION_NAME);
        final String regionWKT = conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        final BinningConfig binningConfig = getL3Config(conf);
        final MetadataElement sourcesMetadata = metadataAggregator.getMetadata();

        final GlobalMetadata globalMetadata = GlobalMetadata.create(binningConfig);
        final MetadataElement processingGraphMetadata = globalMetadata.asMetadataElement();
        final MetadataElement node_0 = processingGraphMetadata.getElement("node.0");
        final MetadataElement parameters = node_0.getElement("parameters");

        addCalvalusMetadata(node_0);
        addCalvalusParameters(parameters, regionName, regionWKT);
        node_0.addElement(sourcesMetadata);

        return processingGraphMetadata;
    }

    private BinningConfig getL3Config(Configuration conf) {
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

    private void addCalvalusMetadata(MetadataElement element) {
        addAttributeToMetadataElement(element, "operator", conf.get(JobConfigNames.CALVALUS_PRODUCTION_TYPE));
        addAttributeToMetadataElement(element, "calvalusVersion", conf.get(JobConfigNames.CALVALUS_CALVALUS_BUNDLE));
        addAttributeToMetadataElement(element, "beamVersion", conf.get(JobConfigNames.CALVALUS_BEAM_BUNDLE));
        addAttributeToMetadataElement(element, "user", conf.get(JobConfigNames.CALVALUS_USER));
    }

    private void addCalvalusParameters(MetadataElement element, String regionName, String regionWKT) {
        addAttributeToMetadataElement(element, "aggregation_period_start", conf.get(JobConfigNames.CALVALUS_MIN_DATE));
        addAttributeToMetadataElement(element, "aggregation_period_end", conf.get(JobConfigNames.CALVALUS_MAX_DATE));
        addAttributeToMetadataElement(element, "region_name", regionName);
        addAttributeToMetadataElement(element, "region", regionWKT);
    }

    private void addAttributeToMetadataElement(MetadataElement parent, String name, String value) {
        if (value != null) {
            parent.addAttribute(new MetadataAttribute(name, ProductData.createInstance(value), true));
        }
    }

}
