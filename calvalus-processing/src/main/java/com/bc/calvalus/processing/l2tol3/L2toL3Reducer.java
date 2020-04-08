/*
 * Copyright (C) 2014 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.l2tol3;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.l3.HadoopBinManager;
import com.bc.calvalus.processing.l3.L3SpatialBin;
import com.bc.calvalus.processing.ma.TaskOutputStreamFactory;
import com.bc.calvalus.processing.utils.GeometryUtils;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.TemporalBin;
import org.esa.snap.binning.TemporalBinner;
import org.esa.snap.binning.operator.BinningConfig;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * Reduces list of spatial bins to a temporal bin.
 *
 * @author Norman Fomferra
 * @author Marco Zuehlke
 */
public class L2toL3Reducer extends Reducer<LongWritable, L3SpatialBin, NullWritable, NullWritable> implements Configurable {

    private Configuration conf;
    private TemporalBinner temporalBinner;
    private Writer writer;
    private String[] featureNames;
    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        writer = new OutputStreamWriter(TaskOutputStreamFactory.createOutputStream(context, filename));
        writer.write("x\tnumPasses\tnumObs\t" + arrayToString(featureNames) + "\n");
    }

    @Override
    protected void reduce(LongWritable binIndex, Iterable<L3SpatialBin> spatialBins, Context context) throws IOException, InterruptedException {
        final long idx = binIndex.get();
        TemporalBin temporalBin = temporalBinner.processSpatialBins(idx, spatialBins);
        temporalBin = temporalBinner.computeOutput(idx, temporalBin);

        int numPasses = temporalBin.getNumPasses();
        int numObs = temporalBin.getNumObs();
        float[] featureValues = temporalBin.getFeatureValues();
        writer.write(binIndex.get() + "\t" + numPasses + "\t" + numObs + "\t" + arrayToString(featureValues) +  "\n");
        context.progress();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writer.close();
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        BinningConfig binningConfig = HadoopBinManager.getBinningConfig(conf);

        Geometry regionGeometry = GeometryUtils.createGeometry(conf.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));
        BinningContext binningContext = HadoopBinManager.createBinningContext(binningConfig, null, regionGeometry);
        temporalBinner = new TemporalBinner(binningContext);
        featureNames = binningContext.getBinManager().getResultFeatureNames();
        conf.setStrings(JobConfigNames.CALVALUS_L3_FEATURE_NAMES, featureNames);

        String prefix = conf.get(JobConfigNames.CALVALUS_OUTPUT_PREFIX);
        filename = prefix + "_" + conf.get(JobConfigNames.CALVALUS_MIN_DATE) + ".csv";
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static String arrayToString(float[] strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append("\t");
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }

    public static String arrayToString(String[] strs) {
        if (strs.length == 0) {
            return "";
        }
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append("\t");
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }
}
