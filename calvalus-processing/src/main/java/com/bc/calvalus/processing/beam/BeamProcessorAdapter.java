/*
 * Copyright (C) 2012 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.processing.beam;

import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.hadoop.ProductSplitProgressMonitor;
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.core.Assert;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;
import org.esa.beam.gpf.operators.standard.SubsetOp;
import org.esa.beam.util.io.FileUtils;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A processor adapter that uses a BEAM GPF {@code Operator} to process an input product.
 *
 * @author MarcoZ
 */
public class BeamProcessorAdapter extends ProcessorAdapter {

    private static final int DEFAULT_TILE_HEIGHT = 64;

    private Product targetProduct;

    public BeamProcessorAdapter(MapContext mapContext) {
        super(mapContext);
        GpfUtils.init(mapContext.getConfiguration());
    }

    @Override
    public String[] getProcessedProductPathes() {
        String inputFilename = getInputPath().getName();
        String outputFilename = "L2_of_" + FileUtils.exchangeExtension(inputFilename, ".seq");
        return new String[]{ outputFilename };
    }

    @Override
    public boolean processSourceProduct(Rectangle srcProductRect) throws IOException {
        Assert.argument(srcProductRect == null || !srcProductRect.isEmpty(), "srcProductRect can not be empty");
        Configuration conf = getConfiguration();
        String processorName = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        Product subsetProduct = createSubset(srcProductRect);
        targetProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);
        if (targetProduct == null ||
                targetProduct.getSceneRasterWidth() == 0 ||
                targetProduct.getSceneRasterHeight() == 0) {
            return false;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                                       targetProduct.getSceneRasterWidth(),
                                       targetProduct.getSceneRasterHeight()));
        copyTimeCoding(subsetProduct, targetProduct);
        return true;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

    @Override
    public void saveProcessedProducts() throws Exception {
        MapContext mapContext = getMapContext();
        String inputFilename = getInputPath().getName();
        String outputFilename = "L2_of_" + FileUtils.exchangeExtension(inputFilename, ".seq");

        Path workOutputProductPath = new Path(FileOutputFormat.getWorkOutputPath(mapContext), outputFilename);
        int tileHeight = DEFAULT_TILE_HEIGHT;
        Dimension preferredTileSize = targetProduct.getPreferredTileSize();
        if (preferredTileSize != null) {
            tileHeight = preferredTileSize.height;
        }
        ProgressMonitor progressMonitor = new ProductSplitProgressMonitor(mapContext);
        StreamingProductWriter streamingProductWriter = new StreamingProductWriter(getConfiguration(), mapContext, progressMonitor);
        streamingProductWriter.writeProduct(targetProduct, workOutputProductPath, tileHeight);
    }

    @Override
    public void dispose() {
        super.dispose();
        if (targetProduct != null) {
            targetProduct.dispose();
            targetProduct = null;
        }
    }

    private void copyTimeCoding(Product subsetProduct, Product processedProduct) {
        if (processedProduct != subsetProduct) {
            if (processedProduct.getStartTime() == null) {
                processedProduct.setStartTime(subsetProduct.getStartTime());
            }
            if (processedProduct.getEndTime() == null) {
                processedProduct.setEndTime(subsetProduct.getEndTime());
            }
        }
    }

    private Product createSubset(Rectangle srcProductRect) throws IOException {
        Product product = getInputProduct();
        // full region
        if (srcProductRect == null ||
                (srcProductRect.width == product.getSceneRasterWidth() && srcProductRect.height == product.getSceneRasterHeight())) {
            return product;
        }

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(srcProductRect);
        op.setCopyMetadata(false);
        Product subsetProduct = op.getTargetProduct();
        getLogger().info(String.format("Created Subset product width = %d height = %d",
                                       subsetProduct.getSceneRasterWidth(),
                                       subsetProduct.getSceneRasterHeight()));
        return subsetProduct;
    }

    private static Product getProcessedProduct(Product source, String operatorName, String operatorParameters) {
        Product product = source;
        if (operatorName != null && !operatorName.isEmpty()) {
            // transform request into parameter objects
            Map<String, Object> parameterMap;
            try {
                parameterMap = getOperatorParameterMap(operatorName, operatorParameters);
            } catch (BindingException e) {
                throw new IllegalArgumentException("Invalid operator parameters: " + e.getMessage(), e);
            }
            product = GPF.createProduct(operatorName, parameterMap, product);
        }
        return product;
    }

    public static Map<String, Object> getOperatorParameterMap(String operatorName, String level2Parameters) throws BindingException {
        if (level2Parameters == null) {
            return Collections.emptyMap();
        }
        Class<? extends Operator> operatorClass = getOperatorClass(operatorName);
        return new ParameterBlockConverter().convertXmlToMap(level2Parameters, operatorClass);
    }

    private static Class<? extends Operator> getOperatorClass(String operatorName) {
        OperatorSpi operatorSpi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operatorName);
        if (operatorSpi == null) {
            throw new IllegalStateException(String.format("Unknown operator '%s'", operatorName));
        }
        return operatorSpi.getOperatorClass();
    }

}
