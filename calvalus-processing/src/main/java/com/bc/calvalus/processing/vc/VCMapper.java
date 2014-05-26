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

package com.bc.calvalus.processing.vc;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.JobUtils;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.ProcessorFactory;
import com.bc.calvalus.processing.executable.ExecutableProcessorAdapter;
import com.bc.calvalus.processing.executable.KeywordHandler;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.calvalus.processing.l2.ProductFormatter;
import com.bc.calvalus.processing.ma.FilteredRecordSource;
import com.bc.calvalus.processing.ma.GeometryRecordFilter;
import com.bc.calvalus.processing.ma.Header;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.PixelPosProvider;
import com.bc.calvalus.processing.ma.PixelTimeProvider;
import com.bc.calvalus.processing.ma.ProductRecordSource;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import com.bc.calvalus.processing.ma.RecordTransformer;
import com.bc.calvalus.processing.ma.RecordWritable;
import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.core.SubProgressMonitor;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;

import java.awt.Rectangle;
import java.awt.geom.Area;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Vicarious Calibration:
 * - extracts from l1b
 * - differentiation to multiple l1bSTAR products
 * - extracts from all l1bSTAR products
 * - processing to l2
 * - extracts from all l2 products
 *
 * @author Marco Zuehlke
 */
public class VCMapper extends Mapper<NullWritable, NullWritable, Text, RecordWritable> {

    public static final Text HEADER_KEY = new Text("#");

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";
    private static final Logger LOG = CalvalusLogger.getLogger();

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        context.progress();

        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.get(jobConfig);
        maConfig.setCopyInput(false); // insitu data is merge separately
        final Geometry regionGeometry = JobUtils.createGeometry(jobConfig.get(JobConfigNames.CALVALUS_REGION_GEOMETRY));

        // write initial log entry for runtime measurements
        LOG.info(String.format("%s starts processing of split %s", context.getTaskAttemptID(), context.getInputSplit()));

        RecordSource referenceRecordSource = getReferenceRecordSource(maConfig, regionGeometry);
        ProcessorAdapter l2ProcessorAdapter = ProcessorFactory.createAdapter(context);

        final int progressForFindingMatchingReferences = 5 + 5;
        final int progressForDifferentiation = 10;
        final int progressForProcessing = 85;
        ProgressMonitor pm = new ProductSplitProgressMonitor(context);
        pm.beginTask("Vicarious Calibration", progressForFindingMatchingReferences + progressForDifferentiation + progressForProcessing);

        try {
            // find matching reference Records
            Path inputPath = l2ProcessorAdapter.getInputPath();
            l2ProcessorAdapter.copyFileToLocal(inputPath);
            File l1LocalFile = l2ProcessorAdapter.getInputFile();
            Product inputProduct = l2ProcessorAdapter.getInputProduct();
            Header referenceRecordHeader = referenceRecordSource.getHeader();
            PixelPosProvider pixelPosProvider = new PixelPosProvider(inputProduct,
                                                                     PixelTimeProvider.create(inputProduct),
                                                                     maConfig.getMaxTimeDifference(),
                                                                     referenceRecordHeader.hasTime());
            Iterable<Record> referenceRecords;
            try {
                referenceRecords = referenceRecordSource.getRecords();
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve input records.", e);
            }
            Area area = new Area();
            int macroPixelSize = maConfig.getMacroPixelSize();

            List<Record> matchingReferenceRecords = new ArrayList<Record>();
            for (Record referenceRecord : referenceRecords) {
                PixelPos pixelPos = pixelPosProvider.getPixelPos(referenceRecord);
                if (pixelPos != null) {
                    Rectangle rectangle = new Rectangle((int) pixelPos.x - macroPixelSize / 2,
                                                        (int) pixelPos.y - macroPixelSize / 2,
                                                        macroPixelSize, macroPixelSize);
                    area.add(new Area(rectangle));
                    matchingReferenceRecords.add(referenceRecord);
                    System.out.println("found matching reference = " + referenceRecord);
                    System.out.println("-- at pixelPos = " + pixelPos);
                }
            }
            pm.worked(5);

            if (!area.isEmpty()) {
                List<NamedRecordSource> namedRecordSources = new ArrayList<NamedRecordSource>();

                // save Level 1 product
                saveLevel1Product(l1LocalFile, context);

                // add in-situ records
                NamedRecordSource matchingReferenceRecordSource = new NamedRecordSource("insitu_", referenceRecordHeader, matchingReferenceRecords);
                namedRecordSources.add(matchingReferenceRecordSource);

                // extract Level 1 match-ups
                ProgressMonitor l1ExtractionPM = SubProgressMonitor.create(pm, 5);
                namedRecordSources.add(getMatchups("L1_", maConfig, l1ExtractionPM, matchingReferenceRecordSource, inputProduct));

                // Differentiation processing
                ExecutableProcessorAdapter differentiationProcessorAdapter = new ExecutableProcessorAdapter(context, VCWorkflowItem.DIFFERENTIATION_SUFFIX);
                ProgressMonitor differentiationPM = SubProgressMonitor.create(pm, progressForDifferentiation);
                KeywordHandler keywordHandler = differentiationProcessorAdapter.process(differentiationPM,
                                                                                        null,
                                                                                        inputPath,
                                                                                        l1LocalFile,
                                                                                        null,
                                                                                        null);
                KeywordHandler.NamedOutput[] namedOutputs = keywordHandler.getNamedOutputFiles();

                if (namedOutputs.length == 0) {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without differentiations").increment(1);
                    return;
                }
                Rectangle fullScene = new Rectangle(inputProduct.getSceneRasterWidth(),
                                                    inputProduct.getSceneRasterHeight());
                Rectangle maRectangle = area.getBounds();
                maRectangle.grow(20, 20); // grow relevant area to have a bit surrounding product content
                Rectangle processingRectangle = fullScene.intersection(maRectangle);
                LOG.info("processing rectangle: " + processingRectangle);
                l2ProcessorAdapter.setProcessingRectangle(processingRectangle);

                // handle all product produced by the differentiation processor
                ProgressMonitor mainLoopPM = SubProgressMonitor.create(pm, progressForProcessing);
                mainLoopPM.beginTask("Level 2", namedOutputs.length * (1 + 5 + 30 + 5 + 1) + (30 + 5 + 1));
                for (KeywordHandler.NamedOutput namedOutput : namedOutputs) {
                    context.progress();

                    // save differentiation product
                    File l1DiffFile = saveDifferentiationProduct(namedOutput, context);
                    mainLoopPM.worked(1);

                    Product l1DiffProduct = ProductIO.readProduct(namedOutput.getFile());

                    // extract differentiation match-ups
                    String diffPrefix = namedOutput.getName() + "_";
                    NamedRecordSource differentiationMatchups = extractMatchups(context, maConfig, matchingReferenceRecordSource, mainLoopPM, l1DiffProduct, diffPrefix);
                    if (differentiationMatchups == null) {
                        return;
                    } else {
                        namedRecordSources.add(differentiationMatchups);
                    }

                    //  Level 2 processing
                    LOG.info("Processing to Level 2: " + l1DiffFile);
                    l2ProcessorAdapter.closeInputProduct();
                    l2ProcessorAdapter.setInputfile(l1DiffFile);
                    l2ProcessorAdapter.processSourceProduct(SubProgressMonitor.create(mainLoopPM, 30));
                    Product l2Product = l2ProcessorAdapter.openProcessedProduct();

                    // extract Level 2 match-ups
                    String l2Prefix = "L2_" + namedOutput.getName()+ "_";
                    NamedRecordSource l2Matchups = extractMatchups(context, maConfig, matchingReferenceRecordSource, mainLoopPM, l2Product, l2Prefix);
                    if (l2Matchups == null) {
                        return;
                    } else {
                        namedRecordSources.add(l2Matchups);
                    }

                    if (jobConfig.getBoolean("calvalus.vc.outputL2", false)) {
                        // TODO handle operators and graphs
                        l2ProcessorAdapter.saveProcessedProducts(SubProgressMonitor.create(mainLoopPM, 1));
                    }
                }
                //  Level 2 processing of primary product
                LOG.info("Processing to Level 2: " + l1LocalFile);
                l2ProcessorAdapter.closeInputProduct();
                l2ProcessorAdapter.setInputfile(l1LocalFile);
                l2ProcessorAdapter.processSourceProduct(SubProgressMonitor.create(mainLoopPM, 30));
                Product l2Product = l2ProcessorAdapter.openProcessedProduct();

                // extract Level 2 match-ups
                String l2Prefix = "L2_";
                NamedRecordSource baseL2Matchups = extractMatchups(context, maConfig, matchingReferenceRecordSource, mainLoopPM, l2Product, l2Prefix);
                if (baseL2Matchups == null) {
                    return;
                }

                if (jobConfig.getBoolean("calvalus.vc.outputL2", false)) {
                    // TODO handle operators and graphs
                    l2ProcessorAdapter.saveProcessedProducts(SubProgressMonitor.create(mainLoopPM, 1));
                }
                mainLoopPM.done();

                MergedRecordSource mergedRecordSource = new MergedRecordSource(matchingReferenceRecordSource, baseL2Matchups, namedRecordSources);
                logAttributeNames(mergedRecordSource);

                // write merged records
                int numMatchUps = 0;
                Iterable<Record> records = mergedRecordSource.getRecords();
                for (Record selectedRecord : records) {
                    Text key = new Text(String.format("%s_%06d", inputProduct.getName(), ++numMatchUps));
                    RecordWritable value = new RecordWritable(selectedRecord.getAttributeValues(), selectedRecord.getAnnotationValues());
                    context.write(key, value);
                    context.progress();
                }

                if (numMatchUps > 0) {
                    // write header
                    Header header = mergedRecordSource.getHeader();
                    context.write(HEADER_KEY, new RecordWritable(header.getAttributeNames(), header.getAnnotationNames()));
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products with match-ups").increment(1);
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Number of match-ups").increment(numMatchUps);
                } else {
                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
                }
            } else {
                context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Products without match-ups").increment(1);
            }
            context.progress();
        } finally {
            pm.done();
            l2ProcessorAdapter.dispose();
        }

    }

    private NamedRecordSource extractMatchups(Context context, MAConfig maConfig, NamedRecordSource matchingReferenceRecordSource, ProgressMonitor mainLoopPM, Product product, String prefix) {
        if (product == null) {
            LOG.info("Product is null: " +  prefix);
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Unused products").increment(1);
        } else {
            try {
                context.progress();
                ProgressMonitor extractionPM = SubProgressMonitor.create(mainLoopPM, 5);
                return getMatchups(prefix, maConfig, extractionPM, matchingReferenceRecordSource, product);
            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve records from product: " + prefix, e);
            }
        }
        return null;
    }

    private void saveLevel1Product(File l1LocalFile, Context context) throws IOException {
        if (context.getConfiguration().getBoolean("calvalus.vc.outputL1", false)) {
            System.out.println("Saving l1 product = " + l1LocalFile.getName());
            saveProduct(context, l1LocalFile);
        }
    }

    private File saveDifferentiationProduct(KeywordHandler.NamedOutput namedOutput, Context context) throws IOException {
        File l1DiffFile = new File(namedOutput.getFile());
        if (context.getConfiguration().getBoolean("calvalus.vc.outputL1Diff", false)) {
            System.out.println("Saving l1-diff product = " + l1DiffFile.getName());
            saveProduct(context, l1DiffFile);
        }
        return l1DiffFile;
    }

    private void saveProduct(Context context, File l1DiffFile) throws IOException {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(l1DiffFile));
        OutputStream outputStream = ProductFormatter.createOutputStream(context, l1DiffFile.getName());
        ProductFormatter.copyAndClose(inputStream, outputStream, context);
    }

    private NamedRecordSource getMatchups(String prefix, MAConfig maConfig, ProgressMonitor extractionPM, NamedRecordSource matchingReferenceRecordSource, Product product) {
        System.out.println("prefix = " + prefix);
        extractionPM.beginTask("Extract Matchups", matchingReferenceRecordSource.getNumRecords());
        try {
            ProductRecordSource productRecordSource = new ProductRecordSource(product, matchingReferenceRecordSource, maConfig);
            Iterable<Record> extractedRecords;
            try {
                extractedRecords = productRecordSource.getRecords();
            } catch (Exception e) {
                throw new RuntimeException("Failed to extract records.", e);
            }
            Header header = productRecordSource.getHeader();
            RecordTransformer recordAggregator = ProductRecordSource.createAggregator(header, maConfig);
            List<Record> aggregatedRecords = new ArrayList<Record>();
            for (Record extractedRecord : extractedRecords) {
                Record aggregatedRecord = recordAggregator.transform(extractedRecord);
                System.out.println("aggregatedRecord = " + aggregatedRecord);
                aggregatedRecords.add(aggregatedRecord);
                extractionPM.worked(1);
            }
            System.out.println("aggregatedRecords.size=" + aggregatedRecords.size());
            return new NamedRecordSource(prefix, header, aggregatedRecords);
        } finally {
            extractionPM.done();
        }
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

    private void logAttributeNames(RecordSource productRecordSource) {
        String[] attributeNames = productRecordSource.getHeader().getAttributeNames();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attributeNames.length; i++) {
            sb.append(String.format("  attributeNames[%d] = \"%s\"\n", i, attributeNames[i]));
        }
        LOG.info("Attribute names:\n" + sb);
    }
}
