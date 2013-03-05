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
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.awt.Rectangle;
import java.awt.geom.Area;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Reads an N1 product and emits records (binIndex, spatialBin).
 *
 * @author Norman Fomferra
 */
public class MAMapper extends Mapper<NullWritable, NullWritable, Text, RecordWritable> {

    public static final Text HEADER_KEY = new Text("#");

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();
    private static final int MiB = 1024 * 1024;

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        context.progress();

        final FileSplit split = (FileSplit) context.getInputSplit();
        final Path inputPath = split.getPath();

        final long mapperStartTime = now();

        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.get(jobConfig);
        final Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));

        // write initial log entry for runtime measurements
        LOG.info(String.format("%s starts processing of split %s (%s MiB)",
                               context.getTaskAttemptID(), split, (MiB / 2 + split.getLength()) / MiB));

        RecordSource referenceRecordSource = getReferenceRecordSource(maConfig, regionGeometry);

        long t0;

        t0 = now();
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Match-Up analysis", 100);
        try {
            Product inputProduct = processorAdapter.getInputProduct();
            PixelPosProvider pixelPosProvider = new PixelPosProvider(inputProduct,
                                                                     PixelTimeProvider.create(inputProduct),
                                                                     maConfig.getMaxTimeDifference(),
                                                                     referenceRecordSource.getHeader().hasTime());
            Iterable<Record> records;
            try {
                records = referenceRecordSource.getRecords();
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve input records.", e);
            }
            boolean containsData = false;
            Area area = new Area();
            int macroPixelSize = maConfig.getMacroPixelSize();

            for (Record record : records) {
                PixelPos pixelPos = pixelPosProvider.getPixelPos(record);
                if (pixelPos != null) {
                    Rectangle rectangle = new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                                                        (int) pixelPos.y - macroPixelSize / 2,
                                                        macroPixelSize, macroPixelSize);
                    area.add(new Area(rectangle));
                    containsData = true;
                }
            }

            if (containsData && !area.isEmpty()) {
                if (!processorAdapter.supportsPullProcessing()) {
                    Rectangle fullScene = new Rectangle(inputProduct.getSceneRasterWidth(),
                                                        inputProduct.getSceneRasterHeight());
                    Rectangle maRectangle = area.getBounds();
                    maRectangle.grow(20, 20); // grow relevant area to have a bit surrounding product content
                    processorAdapter.setProcessingRectangle(fullScene.intersection(maRectangle));
                }
                Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, 50));
                if (product == null) {
                    LOG.info("Processed product is null!");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Unused products").increment(1);
                    return;
                }

                // Actually wrong name for processed products, but we need the field "source_name" in the export data table
                product.setName(FileUtils.getFilenameWithoutExtension(inputPath.getName()));

                context.progress();
                long productOpenTime = (now() - t0);
                LOG.info(String.format("%s opened product %s, took %s sec",
                                       context.getTaskAttemptID(), product.getName(), productOpenTime / 1E3));

                t0 = now();
                ProductRecordSource productRecordSource;
                Iterable<Record> extractedRecords;
                try {
                    //re-create referenceRecordSource to read from the beginning (TODO could be improved)
                    referenceRecordSource = getReferenceRecordSource(maConfig, regionGeometry);
                    productRecordSource = new ProductRecordSource(product, referenceRecordSource, maConfig);
                    extractedRecords = productRecordSource.getRecords();
                    context.progress();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to retrieve input records.", e);
                }

                long recordReadTime = (now() - t0);
                LOG.info(String.format("%s read input records from %s, took %s sec",
                                       context.getTaskAttemptID(), maConfig.getRecordSourceUrl(),
                                       recordReadTime / 1E3));
                logAttributeNames(productRecordSource);

                Header header = productRecordSource.getHeader();
                Rectangle inputRect = processorAdapter.getInputRectangle();
                RecordTransformer productOffsetTransformer = ProductRecordSource.createShiftTransformer(header,
                                                                                                        inputRect);
                RecordTransformer recordTransformer = ProductRecordSource.createAggregator(header, maConfig);
                RecordFilter recordFilter = ProductRecordSource.createRecordFilter(header, maConfig);

                t0 = now();
                int numMatchUps = 0;
                for (Record extractedRecord : extractedRecords) {
                    Record shiftedRecord = productOffsetTransformer.transform(extractedRecord);
                    Record aggregatedRecord = recordTransformer.transform(shiftedRecord);
                    if (aggregatedRecord != null && recordFilter.accept(aggregatedRecord)) {
                        context.write(new Text(String.format("%s_%06d", product.getName(), numMatchUps + 1)),
                                      new RecordWritable(aggregatedRecord.getAttributeValues()));
                        numMatchUps++;
                        context.progress();
                    }
                }
                long recordWriteTime = (now() - t0);
                LOG.info(String.format("%s found %s match-ups, took %s sec",
                                       context.getTaskAttemptID(), numMatchUps, recordWriteTime / 1E3));
                if (numMatchUps > 0) {
                    // write header
                    context.write(HEADER_KEY, new RecordWritable(header.getAttributeNames()));
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products with match-ups").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Number of match-ups").increment(numMatchUps);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
                }
            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
            }

            t0 = now();
            context.progress();
        } finally {
            pm.done();
            processorAdapter.dispose();
        }

        long productCloseTime = (now() - t0);
        LOG.info(String.format("%s closed input product, took %s sec",
                               context.getTaskAttemptID(), productCloseTime / 1E3));

        // write final log entry for runtime measurements
        long mapperTotalTime = (now() - mapperStartTime);
        LOG.info(String.format("%s stops processing of split %s after %s sec",
                               context.getTaskAttemptID(), split, mapperTotalTime / 1E3));

    }

    private RecordSource getReferenceRecordSource(MAConfig maConfig, Geometry regionGeometry) {
        final RecordSource referenceRecordSource;
        try {
            referenceRecordSource = maConfig.createRecordSource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (regionGeometry == null) {
            return referenceRecordSource;
        }
        return new FilteredRecordSource(referenceRecordSource, new GeometryRecordFilter(regionGeometry));
    }

    private void logAttributeNames(ProductRecordSource productRecordSource) {
        String[] attributeNames = productRecordSource.getHeader().getAttributeNames();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attributeNames.length; i++) {
            sb.append(String.format("  attributeNames[%d] = \"%s\"\n", i, attributeNames[i]));
        }
        LOG.info("Attribute names:\n" + sb);
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
