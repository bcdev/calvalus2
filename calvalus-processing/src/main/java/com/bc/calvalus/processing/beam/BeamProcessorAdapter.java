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
import com.bc.ceres.binding.BindingException;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.esa.beam.framework.gpf.Operator;
import org.esa.beam.framework.gpf.OperatorSpi;
import org.esa.beam.framework.gpf.annotations.ParameterBlockConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A processor adapter that uses a BEAM GPF {@code Operator} to process an input product.
 *
 * @author MarcoZ
 */
public class BeamProcessorAdapter extends SubsetProcessorAdapter {

    private Product targetProduct;

    public BeamProcessorAdapter(MapContext mapContext) {
        super(mapContext);
    }

    @Override
    public int processSourceProduct(ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("BEAM Level 2");
        Configuration conf = getConfiguration();
        String processorName = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
        String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

        Product subsetProduct = createSubset();
        String miniWidthConfig = conf.get(JobConfigNames.CALVALUS_INPUT_MIN_WIDTH);
        String minHeightConfig = conf.get(JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT);
        int minWidth = miniWidthConfig != null ? Integer.parseInt(miniWidthConfig) : 0;
        int minHeight = minHeightConfig != null ? Integer.parseInt(minHeightConfig) : 0;
        if (subsetProduct.getSceneRasterWidth() < minWidth && subsetProduct.getSceneRasterHeight() < minHeight) {
            String msgPattern = "The size of the intersection of the product with the region is very small [%d, %d]." +
                                "It will be suppressed from the processing.";
            getLogger().info(String.format(msgPattern, subsetProduct.getSceneRasterWidth(),
                                           subsetProduct.getSceneRasterHeight()));
            return 0;
        }
        targetProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);
        if (targetProduct == null ||
            targetProduct.getSceneRasterWidth() == 0 ||
            targetProduct.getSceneRasterHeight() == 0) {
            return 0;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                                       targetProduct.getSceneRasterWidth(),
                                       targetProduct.getSceneRasterHeight()));
        copyTimeCoding(subsetProduct, targetProduct);
        return 1;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws Exception {
        saveTargetProduct(targetProduct, pm);
    }

    @Override
    public void dispose() {
        super.dispose();
        if (targetProduct != null) {
            targetProduct.dispose();
            targetProduct = null;
        }
    }

    public static void copyTimeCoding(Product sourceProduct, Product targetProduct) {
        if (targetProduct != sourceProduct) {
            if (targetProduct.getStartTime() == null) {
                targetProduct.setStartTime(sourceProduct.getStartTime());
            }
            if (targetProduct.getEndTime() == null) {
                targetProduct.setEndTime(sourceProduct.getEndTime());
            }
        }
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

    public static Map<String, Object> getOperatorParameterMap(String operatorName, String level2Parameters) throws
                                                                                                            BindingException {
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
