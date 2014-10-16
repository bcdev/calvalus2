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

package com.bc.calvalus.processing.fex;

import com.bc.calvalus.processing.l2.ProductFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;
import org.esa.pfa.db.DsIndexer;
import org.esa.pfa.db.DsIndexerTool;
import org.esa.pfa.fe.PFAApplicationDescriptor;
import org.esa.pfa.fe.op.DatasetDescriptor;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This reducer takes the features and creates a lucene Index.
 */
public class FexReducer extends Reducer<Text, Text, Text, Text> {

    private File datasetDir;
    private DsIndexer dsIndexer;
    private Writer csvWriter;
    private boolean headerWritten;
    private List<String> featurePropertyNames;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        PFAApplicationDescriptor applicationDescriptor = FexMapper.getApplicationDescriptor(conf);
        URI dsURI = applicationDescriptor.getDatasetDescriptorURI();
        DatasetDescriptor datasetDescriptor;
        try (Reader inputStreamReader = new InputStreamReader(dsURI.toURL().openStream())) {
            datasetDescriptor = DatasetDescriptor.read(inputStreamReader);
        }

        datasetDir = new File(".", DsIndexerTool.DEFAULT_INDEX_NAME);
        Directory indexDirectory = FSDirectory.open(datasetDir);
        IndexWriterConfig config = DsIndexer.createConfig(1, true);

        dsIndexer = new DsIndexer(datasetDescriptor, NumericUtils.PRECISION_STEP_DEFAULT, indexDirectory, config);
        csvWriter = new OutputStreamWriter(FexMapper.createOutputStream(context, "features.csv"));
        headerWritten = false;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String[] splits = key.toString().split(":");
        String productName = splits[0];
        int patchX = Integer.parseInt(splits[1]);
        int patchY = Integer.parseInt(splits[2]);

        Properties featureProperties = new Properties();
        try (Reader reader = new StringReader(values.iterator().next().toString())) {
            featureProperties.load(reader);
        }

        dsIndexer.addPatchToIndex(productName, patchX, patchY, featureProperties);

        if (!headerWritten) {
            csvWriter.write(getHeader(featureProperties));
        }
        csvWriter.write(getRecord(productName, patchX, patchY, featureProperties));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        dsIndexer.close();
        csvWriter.close();

        OutputStream outputStream = FexMapper.createOutputStream(context, "lucene-index.zip");
        ProductFormatter.zip(datasetDir, outputStream, context);
    }

    private String getHeader(Properties featureProperties) {
        featurePropertyNames = new ArrayList<>(featureProperties.stringPropertyNames());

        StringBuilder sb = new StringBuilder();
        sb.append("productName\tpatchX\tpatchY");
        for (String featureName : featurePropertyNames) {
            sb.append("\t");
            sb.append(featureName);
        }
        return sb.toString();
    }

    private String getRecord(String productName, int patchX, int patchY, Properties featureProperties) {
        StringBuilder sb = new StringBuilder();
        sb.append(productName);
        sb.append("\t");
        sb.append(patchX);
        sb.append("\t");
        sb.append(patchY);
        sb.append("\t");
        for (String featureName : featurePropertyNames) {
            sb.append("\t");
            sb.append(featureProperties.getProperty(featureName));
        }
        return sb.toString();
    }
}
