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
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.util.io.FileUtils;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.Area;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
    public static final String EXCLUSION_REASON_EXPRESSION = "RECORD_EXPRESSION";

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


        long t0;

        t0 = now();
        ProcessorAdapter processorAdapter = ProcessorFactory.createAdapter(context);
        boolean pullProcessing = processorAdapter.supportsPullProcessing();
        final int progressForProcessing = pullProcessing ? 20 : 80;
        final int progressForSaving = maConfig.getSaveProcessedProducts() ? (pullProcessing ? 80 : 20) : 0;
        final int progressForExtraction = pullProcessing ? 80 : 20;
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Match-Up analysis", progressForProcessing + progressForSaving + progressForExtraction);
        ProgressMonitor extractionPM = SubProgressMonitor.create(pm, progressForExtraction);
        try {
            Product inputProduct = processorAdapter.getInputProduct();
            long productOpenTime = (now() - t0);
            LOG.info(String.format("%s opened input product %s, took %s sec",
                                   context.getTaskAttemptID(), inputProduct.getName(), productOpenTime / 1E3));

            t0 = now();
            RecordSource referenceRecordSource = getReferenceRecordSource(maConfig, regionGeometry);
            Header referenceRecordHeader = referenceRecordSource.getHeader();
            PixelPosProvider pixelPosProvider = new PixelPosProvider(inputProduct,
                                                                     PixelTimeProvider.create(inputProduct),
                                                                     maConfig.getMaxTimeDifference(),
                                                                     referenceRecordHeader.hasTime());

            try {
                pixelPosProvider.computePixelPosRecords(referenceRecordSource.getRecords(), maConfig.getMacroPixelSize());
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve input records.", e);
            }
            List<PixelPosProvider.PixelPosRecord> pixelPosRecords = pixelPosProvider.getPixelPosRecords();
            Area pixelArea = pixelPosProvider.getPixelArea();

            long referencePixelTime = (now() - t0);
            LOG.info(String.format("tested reference records, found %s matches, took %s sec",
                                   pixelPosRecords.size(), referencePixelTime / 1E3));

            if (!pixelArea.isEmpty()) {
                t0 = now();
                if (!pullProcessing) {
                    Rectangle fullScene = new Rectangle(inputProduct.getSceneRasterWidth(),
                                                        inputProduct.getSceneRasterHeight());
                    Rectangle maRectangle = pixelArea.getBounds();
                    maRectangle.grow(20, 20); // grow relevant pixelArea to have a bit surrounding product content
                    Rectangle processingRectangle = fullScene.intersection(maRectangle);
                    LOG.info("processing rectangle: " + processingRectangle);
                    processorAdapter.setProcessingRectangle(processingRectangle);
                }
                Product product = processorAdapter.getProcessedProduct(SubProgressMonitor.create(pm, progressForProcessing));
                if (product == null) {
                    LOG.info("Processed product is null!");
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Unused products").increment(1);
                    return;
                }
                if (maConfig.getSaveProcessedProducts()) {
                    processorAdapter.saveProcessedProducts(SubProgressMonitor.create(pm, progressForSaving));
                }

                // Actually wrong name for processed products, but we need the field "source_name" in the export data table
                product.setName(FileUtils.getFilenameWithoutExtension(inputPath.getName()));

                context.progress();
                productOpenTime = (now() - t0);
                LOG.info(String.format("opened processed product %s, took %s sec", product.getName(), productOpenTime / 1E3));

                t0 = now();
                extractionPM.beginTask("Extraction", pixelPosRecords.size() * 2);
                ProductRecordSource productRecordSource;
                Iterable<Record> extractedRecords;
                try {
                    AffineTransform transform = processorAdapter.getInput2OutputTransform();
                    if (transform == null) {
                        transform = new AffineTransform();
                        referenceRecordSource = getReferenceRecordSource(maConfig, regionGeometry);
                        pixelPosProvider = new PixelPosProvider(product,
                                                                PixelTimeProvider.create(product),
                                                                maConfig.getMaxTimeDifference(),
                                                                referenceRecordHeader.hasTime());

                        try {
                            pixelPosProvider.computePixelPosRecords(referenceRecordSource.getRecords(), maConfig.getMacroPixelSize());
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to retrieve input records.", e);
                        }
                        pixelPosRecords = pixelPosProvider.getPixelPosRecords();
                    }
                    productRecordSource = new ProductRecordSource(product, referenceRecordHeader, pixelPosRecords, maConfig, transform);
                    extractedRecords = productRecordSource.getRecords();
                    context.progress();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to retrieve input records.", e);
                }

                long recordReadTime = (now() - t0);
                LOG.info(String.format("read input records from %s, took %s sec",
                                       maConfig.getRecordSourceUrl(),
                                       recordReadTime / 1E3));
                logAttributeNames(productRecordSource);

                Header header = productRecordSource.getHeader();
                RecordTransformer recordAggregator = ProductRecordSource.createAggregator(header, maConfig);
                RecordFilter recordFilter = ProductRecordSource.createRecordFilter(header, maConfig);
                RecordSelector recordSelector = productRecordSource.createRecordSelector();

                t0 = now();
                Collection<Record> aggregatedRecords = new ArrayList<Record>();
                int exclusionIndex = header.getAnnotationIndex(DefaultHeader.ANNOTATION_EXCLUSION_REASON);
                for (Record extractedRecord : extractedRecords) {
                    Record aggregatedRecord = recordAggregator.transform(extractedRecord);
                    String reason = (String) aggregatedRecord.getAnnotationValues()[exclusionIndex];
                    if (reason.isEmpty() && !recordFilter.accept(aggregatedRecord)) {
                        aggregatedRecord.getAnnotationValues()[exclusionIndex] = EXCLUSION_REASON_EXPRESSION;
                    }
                    aggregatedRecords.add(aggregatedRecord);
                    extractionPM.worked(1);
                }

                Iterable<Record> selectedRecords = recordSelector.select(aggregatedRecords);
                int numMatchUps = 0;
                for (Record selectedRecord : selectedRecords) {
                    context.write(new Text(String.format("%06d_%s", selectedRecord.getId(), product.getName())),
                                  new RecordWritable(selectedRecord.getAttributeValues(), selectedRecord.getAnnotationValues()));
                    context.progress();
                    extractionPM.worked(1);
                    numMatchUps++;
                }

                long recordWriteTime = (now() - t0);
                LOG.info(String.format("found %s match-ups, took %s sec", numMatchUps, recordWriteTime / 1E3));
                if (numMatchUps > 0) {
                    // write header
                    context.write(HEADER_KEY, new RecordWritable(header.getAttributeNames(), header.getAnnotationNames()));
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
            extractionPM.done();
            pm.done();
            processorAdapter.dispose();
        }

        long productCloseTime = (now() - t0);
        LOG.info(String.format("closed input product, took %s sec", productCloseTime / 1E3));

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
