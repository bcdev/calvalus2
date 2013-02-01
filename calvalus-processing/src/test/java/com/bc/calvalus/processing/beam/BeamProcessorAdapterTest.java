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
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.framework.gpf.GPF;
import org.junit.Test;

import java.awt.Rectangle;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author Norman
 */
public class BeamProcessorAdapterTest {

    @Test
    public void testFullProduct() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        BeamProcessorAdapter beamProcessorAdapter = createProcessorAdapter(productSplit, sourceProduct, null);

        int numProducts = beamProcessorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = beamProcessorAdapter.openProcessedProduct();

        assertSame(sourceProduct, targetProduct);
    }

    @Test
    public void testSubset() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        BeamProcessorAdapter beamProcessorAdapter = createProcessorAdapter(productSplit, sourceProduct,
                                                                           new Rectangle(10, 20));

        int numProducts = beamProcessorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = beamProcessorAdapter.openProcessedProduct();

        assertNotSame(sourceProduct, targetProduct);
        assertEquals(10, targetProduct.getSceneRasterWidth());
        assertEquals(20, targetProduct.getSceneRasterHeight());
    }

    @Test
    public void testSubset_Null() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        BeamProcessorAdapter beamProcessorAdapter = createProcessorAdapter(productSplit, sourceProduct, null);

        int numProducts = beamProcessorAdapter.processSourceProduct(ProgressMonitor.NULL);
        assertEquals(1, numProducts);
        Product targetProduct = beamProcessorAdapter.openProcessedProduct();

        assertSame(sourceProduct, targetProduct);
        assertEquals(100, targetProduct.getSceneRasterWidth());
        assertEquals(100, targetProduct.getSceneRasterHeight());
    }

    @Test
    public void testSubset_EmptyRect() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 0, 0);
        BeamProcessorAdapter beamProcessorAdapter = createProcessorAdapter(productSplit, sourceProduct,
                                                                           new Rectangle());
        try {
            beamProcessorAdapter.processSourceProduct(ProgressMonitor.NULL);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Can not create an empty subset.", e.getMessage());
        }
    }

    @Test
    public void testProductSplit() throws Exception {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product sourceProduct = createSourceProduct();
        ProductSplit productSplit = new ProductSplit(null, 42L, new String[0], 10, 20);
        BeamProcessorAdapter beamProcessorAdapter = createProcessorAdapter(productSplit, sourceProduct);

        Product targetProduct = beamProcessorAdapter.openProcessedProduct();
        Rectangle rectangle = beamProcessorAdapter.getInputRectangle();

        assertNotSame(sourceProduct, targetProduct);
        assertEquals(100, rectangle.width);
        assertEquals(20, rectangle.height);
    }

    private static BeamProcessorAdapter createProcessorAdapter(final InputSplit inputSplit, final Product sourceProduct,
                                                               final Rectangle inputRect) {
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_WIDTH, "0");
        conf.set(JobConfigNames.CALVALUS_INPUT_MIN_HEIGHT, "0");
        conf.set(JobConfigNames.CALVALUS_L2_OPERATOR, "PassThrough");
        conf.set(JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>");
        TaskAttemptID taskid = new TaskAttemptID();
        MapContext mapContext = new MapContextImpl(conf, taskid, null, null, null, null, inputSplit);
        return new BeamProcessorAdapter(mapContext) {
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

    private static BeamProcessorAdapter createProcessorAdapter(final InputSplit inputSplit,
                                                               final Product sourceProduct) {
        Configuration conf = new Configuration();
        conf.set(JobConfigNames.CALVALUS_L2_OPERATOR, "PassThrough");
        conf.set(JobConfigNames.CALVALUS_L2_PARAMETERS, "<parameters/>");
        TaskAttemptID taskid = new TaskAttemptID();
        MapContext mapContext = new MapContextImpl(conf, taskid, null, null, null, null, inputSplit);
        return new BeamProcessorAdapter(mapContext) {
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

}
