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

import java.io.IOException;
import java.util.Map;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L3Reducer extends Reducer<LongWritable, L3SpatialBin, LongWritable, L3TemporalBin> implements Configurable {

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private CellProcessorChain cellChain;
    private boolean computeOutput;
    private StringBuffer outputMetadata = new StringBuffer();

    @Override
    protected void reduce(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        final long idx = binIndex.get();
        if (idx == L3SpatialBin.METADATA_MAGIC_NUMBER) {
            for (L3SpatialBin metadataBin : spatialBins) {
                String inputMetadata = metadataBin.getMetadata();
                // TODO merge inputMetadata into outputMetadata
                if (outputMetadata.length() > 0) {
                    outputMetadata.append("\n");
                }
                outputMetadata.append(inputMetadata);
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
        if (outputMetadata.length() > 0) {
            Path workOutputPath = FileOutputFormat.getWorkOutputPath(context);
            Map<String, String> metadata = ProcessingMetadata.config2metadata(conf, JobConfigNames.LEVEL3_METADATA_KEYS);
            // TODO convert outputMetadata to string, add L3 step
            String history = outputMetadata.toString();
            metadata.put(JobConfigNames.PROCESSING_HISTORY, history);
            ProcessingMetadata.write(workOutputPath, conf, metadata);
            outputMetadata.setLength(0); // TODO is this sufficient in case of re-use
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        computeOutput = conf.getBoolean(JobConfigNames.CALVALUS_L3_COMPUTE_OUTPUTS, true);

        BinningConfig binningConfig = getL3Config(conf);

        Geometry regionGeometry = JobUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, null, regionGeometry);
        temporalBinner = new TemporalBinner(binningContext);
        cellChain = new CellProcessorChain(binningContext);
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, binningContext.getBinManager().getResultFeatureNames());
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

    @Override
    public Configuration getConf() {
        return conf;
    }
}
