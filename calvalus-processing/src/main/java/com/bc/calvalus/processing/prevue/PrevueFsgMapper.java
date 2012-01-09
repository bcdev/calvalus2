/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.prevue;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.beam.ProductFactory;
import com.bc.calvalus.processing.ma.MAConfig;
import com.bc.calvalus.processing.ma.Record;
import com.bc.calvalus.processing.ma.RecordSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.dataio.ProductIO;
import org.esa.beam.framework.dataio.ProductWriter;
import org.esa.beam.framework.datamodel.GeoPos;
import org.esa.beam.framework.datamodel.MetadataElement;
import org.esa.beam.framework.datamodel.PixelPos;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.gpf.operators.standard.reproject.ReprojectionOp;
import org.esa.beam.util.io.FileUtils;

import java.awt.Rectangle;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DecimalFormat;

/**
 * For "Prevue", does data extraction in a special way...
 */
public class PrevueFsgMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    private static final String COUNTER_GROUP_NAME_PRODUCTS = "Products";

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        final FileSplit split = (FileSplit) context.getInputSplit();
        final Path inputPath = split.getPath();

        final Configuration jobConfig = context.getConfiguration();
        final MAConfig maConfig = MAConfig.get(jobConfig);
        final String inputFormat = jobConfig.get(JobConfigNames.CALVALUS_INPUT_FORMAT, null);
        final ProductFactory productFactory = new ProductFactory(jobConfig);

        Product product = productFactory.getProduct(inputPath,
                                                    inputFormat,
                                                    null,
                                                    false, // Don't create subsets for MA, otherwise we get wrong pixel coordinates!
                                                    null,
                                                    null);
        if (product == null) {
            productFactory.dispose();
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Empty products").increment(1);
            return;
        }

        RecordSource recordSource = getReferenceRecordSource(maConfig);

        // Actually wrong name for processed products, but we need the field "source_name" in the export data table
        product.setName(FileUtils.getFilenameWithoutExtension(inputPath.getName()));

        context.progress();

        DecimalFormat decimalFormat = new DecimalFormat("000");
        try {
            for (Record record : recordSource.getRecords()) {
                GeoPos location = record.getLocation();
                PixelPos pixelPos = product.getGeoCoding().getPixelPos(location, null);
                if (product.containsPixel(pixelPos)) {
                    Double id = (Double) record.getAttributeValues()[0];
                    String idAsString = decimalFormat.format(id);

                    SubsetOp subsetOp = new SubsetOp();
                    subsetOp.setCopyMetadata(false);
                    Rectangle pixelRect = new Rectangle((int) pixelPos.x - 50, (int) pixelPos.y - 50, 100, 100);
                    Rectangle productRect = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
                    pixelRect = pixelRect.intersection(productRect);
                    subsetOp.setRegion(pixelRect);
                    subsetOp.setSourceProduct(product);
                    Product subsetProduct = subsetOp.getTargetProduct();

                    ReprojectionOp reprojectionOp = new ReprojectionOp();
                    reprojectionOp.setSourceProduct(subsetProduct);
                    reprojectionOp.setParameter("crs", "AUTO:42001"); // UTM automatic
                    Product utmProduct = reprojectionOp.getTargetProduct();

                    PixelPos utmPixelPos = utmProduct.getGeoCoding().getPixelPos(location, null);

                    SubsetOp subsetOp2 = new SubsetOp();
                    subsetOp2.setCopyMetadata(false);
                    Rectangle utmPixelRect = new Rectangle((int) utmPixelPos.x - 24, (int) utmPixelPos.y - 24, 49, 49);
                    Rectangle utmProductRect = new Rectangle(utmProduct.getSceneRasterWidth(), utmProduct.getSceneRasterHeight());
                    utmPixelRect = utmPixelRect.intersection(utmProductRect);
                    subsetOp2.setRegion(utmPixelRect);
                    subsetOp2.setSourceProduct(utmProduct);
                    Product targetProduct = subsetOp2.getTargetProduct();

                    // NO metadata
                    targetProduct.getMetadataRoot().getElementGroup().removeAll();

                    Path outputDir = new Path(FileOutputFormat.getOutputPath(context), idAsString);
                    Path outputPath = new Path(outputDir, product.getName() + ".txt");

                    FileSystem fileSystem = outputPath.getFileSystem(context.getConfiguration());
                    FSDataOutputStream outputStream = fileSystem.create(outputPath);
                    Writer writer = new OutputStreamWriter(outputStream);

                    ProductWriter ascii = ProductIO.getProductWriter("BEAM-ASCII");
                    ascii.writeProductNodes(targetProduct, writer);
                    ascii.close();

                    context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Written ASCII products").increment(1);

                    subsetOp2.dispose();
                    reprojectionOp.dispose();
                    subsetOp.dispose();
                }
            }
        } catch (Exception e) {
            context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Failed products").increment(1);
            throw new IOException(e);
        } finally {
            productFactory.dispose();
            product.dispose();
        }
        context.getCounter(COUNTER_GROUP_NAME_PRODUCTS, "Used products").increment(1);

    }

    private RecordSource getReferenceRecordSource(MAConfig maConfig) {
        final RecordSource referenceRecordSource;
        try {
            referenceRecordSource = maConfig.createRecordSource();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return referenceRecordSource;
    }


}
