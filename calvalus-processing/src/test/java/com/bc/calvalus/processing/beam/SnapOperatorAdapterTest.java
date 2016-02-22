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
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.ceres.core.ProgressMonitor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.Operator;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.OperatorSpi;
import org.esa.snap.core.gpf.annotations.OperatorMetadata;
import org.esa.snap.core.gpf.annotations.Parameter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.awt.*;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class SnapOperatorAdapterTest {

    private static InternalTestOperator.Spi operatorSpi;

    @BeforeClass
    public static void setUp() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();
        operatorSpi = new InternalTestOperator.Spi();
        GPF.getDefaultInstance().getOperatorSpiRegistry().addOperatorSpi(operatorSpi);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().removeOperatorSpi(operatorSpi);
    }

    @Test
    public void testFullProduct() throws Exception {

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        SnapOperatorAdapter snapOperatorAdapter = createProcessorAdapter(productSplit, sourceProduct, null);

        int numProducts = snapOperatorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = snapOperatorAdapter.openProcessedProduct();

        assertSame(sourceProduct, targetProduct);
    }

    @Test
    public void testInitOfOperatorWithoutL2Parameters() throws Exception {
        final Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_WIDTH, "0");
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT, "0");
        conf.set(JobConfigNames.CALVALUS_L2_OPERATOR, "InternalTestOp");
        TaskAttemptID taskid = new TaskAttemptID();
        MapContext mapContext = new MapContextImpl(conf, taskid, null, null, null, null, productSplit);
        mapContext.getConfiguration().setBoolean("calvalus.snap.setSnapProperties", false);
        SnapOperatorAdapter snapOperatorAdapter = new SnapOperatorAdapter(mapContext) {
            @Override
            public Product getInputProduct() throws IOException {
                return sourceProduct;
            }

            @Override
            public Rectangle getInputRectangle() throws IOException {
                return null;
            }
        };

        snapOperatorAdapter.processSourceProduct(ProgressMonitor.NULL);
    }

    @Test
    public void testSubset() throws Exception {

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        SnapOperatorAdapter snapOperatorAdapter = createProcessorAdapter(productSplit, sourceProduct,
                                                                           new Rectangle(10, 20));

        int numProducts = snapOperatorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = snapOperatorAdapter.openProcessedProduct();

        assertNotSame(sourceProduct, targetProduct);
        assertEquals(10, targetProduct.getSceneRasterWidth());
        assertEquals(20, targetProduct.getSceneRasterHeight());
    }

    @Test
    public void testSubset_Null() throws Exception {

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        SnapOperatorAdapter snapOperatorAdapter = createProcessorAdapter(productSplit, sourceProduct, null);

        int numProducts = snapOperatorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = snapOperatorAdapter.openProcessedProduct();

        assertSame(sourceProduct, targetProduct);
        assertEquals(100, targetProduct.getSceneRasterWidth());
        assertEquals(100, targetProduct.getSceneRasterHeight());
    }

    @Test
    public void testSubset_EmptyRect() throws Exception {

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        SnapOperatorAdapter snapOperatorAdapter = createProcessorAdapter(productSplit, sourceProduct,
                                                                           new Rectangle());
        try {
            snapOperatorAdapter.processSourceProduct(ProgressMonitor.NULL);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Can not create an empty subset.", e.getMessage());
        }
    }

    @Test
    public void testProductSplit() throws Exception {

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 10, 20);
        SnapOperatorAdapter snapOperatorAdapter = createProcessorAdapter(productSplit, sourceProduct);

        Product targetProduct = snapOperatorAdapter.openProcessedProduct();
        Rectangle rectangle = snapOperatorAdapter.getInputRectangle();

        assertNotSame(sourceProduct, targetProduct);
        assertEquals(100, rectangle.width);
        assertEquals(20, rectangle.height);
    }

    private static SnapOperatorAdapter createProcessorAdapter(final InputSplit inputSplit, final Product sourceProduct,
                                                              final Rectangle inputRect) {
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_WIDTH, "0");
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT, "0");
        conf.set(JobConfigNames.CALVALUS_L2_OPERATOR, "PassThrough");
        conf.set(JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>");
        TaskAttemptID taskid = new TaskAttemptID();
        MapContext mapContext = new MapContextImpl(conf, taskid, null, null, null, null, inputSplit);
        mapContext.getConfiguration().setBoolean("calvalus.snap.setSnapProperties", false);
        return new SnapOperatorAdapter(mapContext) {
            @Override
            public Product getInputProduct() throws IOException {
                return sourceProduct;
            }

            @Override
            public Rectangle getInputRectangle() throws IOException {
                return inputRect;
            }
        };
    }

    private static SnapOperatorAdapter createProcessorAdapter(final InputSplit inputSplit,
                                                              final Product sourceProduct) {
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_L2_OPERATOR, "PassThrough");
        conf.set(JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>");
        TaskAttemptID taskid = new TaskAttemptID();
        MapContext mapContext = new MapContextImpl(conf, taskid, null, null, null, null, inputSplit);
        mapContext.getConfiguration().setBoolean("calvalus.snap.setSnapProperties", false);
        return new SnapOperatorAdapter(mapContext) {
            @Override
            public Product getInputProduct() throws IOException {
                return sourceProduct;
            }
        };
    }

    private static Product createSourceProduct() {
        Product sourceProduct = new Product("N", "T", 100, 100);
        sourceProduct.addBand("b1", "1.0");
        sourceProduct.addBand("b2", "2.0");
        return sourceProduct;
    }


    @OperatorMetadata(alias = "InternalTestOp", internal = true)
    public static class InternalTestOperator extends Operator {

        @Parameter(defaultValue = "a+b==c")
        private String expression;

        @Override
        public void initialize() throws OperatorException {
            if (expression == null) {
                throw new OperatorException("expression should not be null. It has a default value.");
            }
            setTargetProduct(new Product("dummy", "dummy", 10, 10));
        }

        public static class Spi extends OperatorSpi {

            public Spi() {
                super(InternalTestOperator.class);
            }

        }

    }
}
