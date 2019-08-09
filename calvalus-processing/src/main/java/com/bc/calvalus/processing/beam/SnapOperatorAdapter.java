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
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorSpi;
import org.esa.snap.core.gpf.annotations.ParameterBlockConverter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A processor adapter that uses a SNAP GPF {@code Operator} to process an input product.
 *
 * @author MarcoZ
 */
public class SnapOperatorAdapter extends SubsetProcessorAdapter {

    private Product targetProduct;

    public SnapOperatorAdapter(MapContext mapContext) {
        super(mapContext);

        if (getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY) != null) {
            if (getConfiguration().get("calvalus.system.snap.dataio.reader.tileHeight") == null) {
                System.setProperty("snap.dataio.reader.tileHeight", "128");
                getLogger().info("Setting tileHeight to 128 for subsetting");
            }
            if ((getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth") == null
                 || "*".equals(getConfiguration().get("calvalus.system.snap.dataio.reader.tileWidth")))
                && ! getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_FULL_SWATH, false)) {
                System.setProperty("snap.dataio.reader.tileWidth", "128");
                getLogger().info("Setting tileWidth to 128 for subsetting");
            }
        }
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("SNAP Level 2");
        try {
            Configuration conf = getConfiguration();
            String processorName = conf.get(JobConfigNames.CALVALUS_L2_OPERATOR);
            String processorParameters = conf.get(JobConfigNames.CALVALUS_L2_PARAMETERS);

            Product sourceProduct;
            // TODO defaults for subsetting preserve old behaviour
            if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_INPUT_SUBSETTING, true)) {
                sourceProduct = createSubset();
            } else {
                sourceProduct = getInputProduct();
            }
            Product subsetProduct = createSubset();
            int minWidth = conf.getInt(JobConfigNames.CALVALUS_INPUT_MIN_WIDTH, 0);
            int minHeight = conf.getInt(JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT, 0);
            if (subsetProduct.getSceneRasterWidth() < minWidth &&
                subsetProduct.getSceneRasterHeight() < minHeight) {
                String msgPattern = "The size of the intersection of the product with the region is very small [%d, %d]." +
                                    "It will be suppressed from the processing.";
                getLogger().info(String.format(msgPattern, subsetProduct.getSceneRasterWidth(),
                                               subsetProduct.getSceneRasterHeight()));
                return false;
            }
            if (getConfiguration().getBoolean(JobConfigNames.CALVALUS_OUTPUT_SUBSETTING, false)) {
                targetProduct = createSubset(getProcessedProduct(subsetProduct, processorName, processorParameters));
            } else {
                targetProduct = getProcessedProduct(subsetProduct, processorName, processorParameters);
            }
            if (targetProduct == null ||
                targetProduct.getSceneRasterWidth() == 0 ||
                targetProduct.getSceneRasterHeight() == 0) {
                getLogger().info("Skip processing");
                return false;
            }
            getLogger().info(String.format("Processed product width = %d height = %d",
                                           targetProduct.getSceneRasterWidth(),
                                           targetProduct.getSceneRasterHeight()));
            if (hasInvalidStartAndStopTime(targetProduct)) {
                copySceneRasterStartAndStopTime(subsetProduct, targetProduct, null);
            }
        } finally {
            pm.done();
        }
        return true;
    }

    @Override
    public Product openProcessedProduct() {
        return targetProduct;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
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

    private Product getProcessedProduct(Product source, String operatorName, String operatorParameters) {
        Product product = source;
        if (operatorName != null && !operatorName.isEmpty()) {
            // transform request into parameter objects
            Map<String, Object> parameterMap;
            try {
                parameterMap = getOperatorParameterMap(operatorName, operatorParameters);
                for (int i=0; i<getInputParameters().length; i+=2) {
                    if (! "output".equals(getInputParameters()[i])) {
                        parameterMap.put(getInputParameters()[i], getInputParameters()[i + 1]);
                    }
                }
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
