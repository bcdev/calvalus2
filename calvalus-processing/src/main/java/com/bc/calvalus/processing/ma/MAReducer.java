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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
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

        final CsvRecordWriter recordWriter = new CsvRecordWriter(createWriter(context, "records-all.txt"),
                                                                 createWriter(context, "records-agg.txt"));

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

        PrintStream summaryFileWriter = new PrintStream(createOutputStream(context, "analysis-summary.xml"));
        summaryFileWriter.print("" +
                                        "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" +
                                        "<?xml-stylesheet type=\"text/xsl\" href=\"analysis-summary.xsl\"?>\n" +
                                        "\n" +
                                        "<analysisSummary>\n");

        summaryFileWriter.println();
        summaryFileWriter.printf("<performedAt>%s</performedAt>\n", new Date());
        summaryFileWriter.printf("<recordCount>%s</recordCount>\n", recordIndex);
        summaryFileWriter.println();

        ArrayList<Map.Entry<String, String>> entries = getConfigurationList(jobConfig);
        summaryFileWriter.println();
        summaryFileWriter.print("<configuration>\n");
        for (Map.Entry<String, String> entry : entries) {
            summaryFileWriter.printf("    <property>\n");
            summaryFileWriter.printf("        <name>%s</name>\n", entry.getKey());
            summaryFileWriter.printf("        <value>%s</value>\n", entry.getValue());
            summaryFileWriter.printf("    </property>\n");
        }
        summaryFileWriter.print("</configuration>\n");
        summaryFileWriter.println();

        final PlotGenerator plotGenerator = new PlotGenerator();
        plotGenerator.setImageWidth(400);
        plotGenerator.setImageHeight(400);
        for (int i = 0; i < plotDatasets.length; i++) {
            PlotDatasetCollector.PlotDataset plotDataset = plotDatasets[i];
            String title = String.format("%s @ %s",
                                         plotDataset.getVariablePair().satelliteAttributeName,
                                         plotDataset.getGroupName());
            String subTitle = String.format("%s, %s",
                                            jobConfig.get(JobConfNames.CALVALUS_L2_BUNDLE),
                                            jobConfig.get(JobConfNames.CALVALUS_L2_OPERATOR));
            String imageFilename = String.format("scatter-plot-%s-%s-%03d.png",
                                                 plotDataset.getGroupName(),
                                                 plotDataset.getVariablePair().satelliteAttributeName,
                                                 i);
            Path outputProductPath = new Path(FileOutputFormat.getWorkOutputPath(context), imageFilename);
            PlotGenerator.Result result = plotGenerator.createResult(title, subTitle, plotDataset);
            FSDataOutputStream outputStream = outputProductPath.getFileSystem(jobConfig).create(outputProductPath);
            try {
                LOG.warning(String.format("Writing %s", outputProductPath));
                ImageIO.write(result.plotImage, "PNG", outputStream);
            } catch (IOException e) {
                LOG.warning(String.format("Failed to write %s: %s", outputProductPath, e.getMessage()));
            } finally {
                outputStream.close();
            }

            summaryFileWriter.printf("<dataset>\n");
            summaryFileWriter.printf("    <referenceVariable>%s</referenceVariable>\n", plotDataset.getVariablePair().referenceAttributeName);
            summaryFileWriter.printf("    <satelliteVariable>%s</satelliteVariable>\n", plotDataset.getVariablePair().referenceAttributeName);
            summaryFileWriter.printf("    <statistics>\n");
            summaryFileWriter.printf("        <numDataPoints>%s</numDataPoints>\n", plotDataset.getPoints().length);
            summaryFileWriter.printf("        <regressionInter>%s</regressionInter>\n", result.regressionCoefficients[0]);
            summaryFileWriter.printf("        <regressionSlope>%s</regressionSlope>\n", result.regressionCoefficients[1]);
            summaryFileWriter.printf("        <scatterPlotImage>%s</scatterPlotImage>\n", imageFilename);
            summaryFileWriter.printf("    </statistics>\n");
            summaryFileWriter.printf("</dataset>\n");
        }

        summaryFileWriter.println("</analysisSummary>");
        summaryFileWriter.close();

        copyResource(context, "analysis-summary.xsl");
    }

    private ArrayList<Map.Entry<String, String>> getConfigurationList(Configuration jobConfig) {
        ArrayList<Map.Entry<String, String>> entries = new ArrayList<Map.Entry<String, String>>(256);
        for (Map.Entry<String, String> entry : jobConfig) {
            entries.add(entry);
        }
        Collections.sort(entries, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });
        return entries;
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

    private Writer createWriter(Context context, String fileName) throws IOException, InterruptedException {
        return new OutputStreamWriter(createOutputStream(context, fileName));
    }

    private FSDataOutputStream createOutputStream(Context context, String fileName) throws IOException, InterruptedException {
        Path path = new Path(FileOutputFormat.getWorkOutputPath(context), fileName);
        return path.getFileSystem(context.getConfiguration()).create(path);
    }

    private void copyResource(Context context, String fileName) throws IOException, InterruptedException {
        InputStream inputStream = getClass().getResourceAsStream(fileName);
        if (inputStream == null) {
            return;
        }
        OutputStream outputStream = createOutputStream(context, fileName);
        try {
            copy(inputStream, outputStream);
        } finally {
            outputStream.close();
            inputStream.close();
        }
    }

    private static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buffer = new byte[64 * 1024];
        while (true) {
            int n = inputStream.read(buffer);
            if (n > 0) {
                outputStream.write(buffer, 0, n);
            } else {
                break;
            }
        }
    }
}
