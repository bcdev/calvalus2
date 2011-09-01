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

package com.bc.calvalus.processing.ma;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfNames;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.logging.Logger;

/**
 * Reads the records emitted by the MAMapper.
 * It is expected that each true 'record' key will only have one unique value.
 * Only 'header' keys ("#") will have multiple values containing all the same the attribute names.
 * This is why the reducer only writes the first value.
 *
 * @author Norman Fomferra
 */
public class MAReducer extends Reducer<Text, RecordWritable, Text, RecordWritable> {
    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.fromXml(jobConfig.get(JobConfNames.CALVALUS_MA_PARAMETERS));
        final String outputGroupName = maConfig.getOutputGroupName();

        final PlotDatasetCollector plotDatasetCollector = new PlotDatasetCollector(outputGroupName);

        final CsvRecordWriter recordWriter = new CsvRecordWriter(createTextFile(context, "records-all.txt"),
                                                                 createTextFile(context, "records-agg.txt"));

        final RecordProcessor[] recordProcessors = new RecordProcessor[]{
                plotDatasetCollector,
                recordWriter,
        };

        LOG.warning("Collecting records...");
        int recordIndex = 0;
        while (context.nextKey()) {
            final Text key = context.getCurrentKey();
            final Iterator<RecordWritable> iterator = context.getValues().iterator();
            if (iterator.hasNext()) {

                final RecordWritable record = iterator.next();
                context.write(key, record);

                if (key.equals(MAMapper.HEADER_KEY)) {
                    processHeaderRecord(record, recordProcessors);
                } else {
                    processDataRecord(recordIndex, record, recordProcessors);
                    recordIndex++;
                }
            }
        }

        finalizeRecordProcessing(recordIndex, recordProcessors);

        final PlotDatasetCollector.PlotDataset[] plotDatasets = plotDatasetCollector.getPlotDatasets();

        LOG.warning(String.format("Generating %d plot(s)...", plotDatasets.length));

        final PlotGenerator plotGenerator = new PlotGenerator();
        plotGenerator.setImageWidth(400);
        plotGenerator.setImageHeight(400);
        for (int i = 0; i < plotDatasets.length; i++) {
            final PlotDatasetCollector.PlotDataset plotDataset = plotDatasets[i];
            final String title = plotDataset.getVariablePair().referenceAttributeName + " / " + plotDataset.getGroupName();
            final String processorTitle = jobConfig.get(JobConfNames.CALVALUS_L2_BUNDLE) + ", " + jobConfig.get(JobConfNames.CALVALUS_L2_OPERATOR);
            final String referenceTitle = new Path(maConfig.getRecordSourceUrl()).getName();
            final BufferedImage plotImage = plotGenerator.createPlotImage(title, processorTitle, referenceTitle, plotDataset);
            final String outputFilename = String.format("scatter-plot-%s-%s-%03d.png", plotDataset.getGroupName(), plotDataset.getVariablePair().satelliteAttributeName, i);
            final Path outputProductPath = new Path(FileOutputFormat.getWorkOutputPath(context), outputFilename);
            final FSDataOutputStream outputStream = outputProductPath.getFileSystem(jobConfig).create(outputProductPath);
            try {
                LOG.warning(String.format("Writing %s", outputProductPath));
                ImageIO.write(plotImage, "PNG", outputStream);
            } catch (IOException e) {
                LOG.warning(String.format("Failed to write %s: %s", outputProductPath, e.getMessage()));
            } finally {
                outputStream.close();
            }
        }
    }

    private void processHeaderRecord(RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processHeaderRecord(record.getValues());
        }
    }

    private void processDataRecord(int recordIndex, RecordWritable record, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.processDataRecord(recordIndex, record.getValues());
        }
    }

    private void finalizeRecordProcessing(int numRecords, RecordProcessor[] recordProcessors) throws IOException {
        for (RecordProcessor recordProcessor : recordProcessors) {
            recordProcessor.finalizeRecordProcessing(numRecords);
        }
    }

    private Writer createTextFile(Context context, String fileName) throws IOException, InterruptedException {
        Path recordsAllPath = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        FSDataOutputStream outputStream = recordsAllPath.getFileSystem(context.getConfiguration()).create(recordsAllPath);
        return new OutputStreamWriter(outputStream);
    }

}
