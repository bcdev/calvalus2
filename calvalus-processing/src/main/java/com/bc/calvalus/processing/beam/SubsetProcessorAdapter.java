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
import com.bc.calvalus.processing.ProcessingRectangleCalculator;
import com.bc.calvalus.processing.ProcessorAdapter;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.bc.calvalus.processing.utils.ProductTransformation;
import com.bc.ceres.core.ProgressMonitor;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.util.io.FileUtils;

import java.awt.Dimension;
import java.awt.Rectangle;
import java.io.IOException;

/**
 * A processor adapter that does not process the input product.
 * It only subsets it using the geometry.
 *
 * @author MarcoZ
 */
public class SubsetProcessorAdapter extends ProcessorAdapter {

    private static final int DEFAULT_TILE_HEIGHT = 64;

    private Product subsetProduct;

    public SubsetProcessorAdapter(MapContext mapContext) {
        super(mapContext);
    }

    @Override
    public boolean canSkipInputProduct() throws IOException {
        Path outputProductPath = new Path(getOutputDirectoryPath(), getOutputProductFilename());
        FileSystem fs = outputProductPath.getFileSystem(getConfiguration());
        return fs.exists(outputProductPath);
    }

    @Override
    public boolean processSourceProduct(MODE mode, ProgressMonitor pm) throws IOException {
        pm.setSubTaskName("L2 Subset");

        subsetProduct = createSubsetFromInput(getInputProduct());
        if (subsetProduct == null ||
                subsetProduct.getSceneRasterWidth() == 0 ||
                subsetProduct.getSceneRasterHeight() == 0) {
            return false;
        }
        getLogger().info(String.format("Processed product width = %d height = %d",
                subsetProduct.getSceneRasterWidth(),
                subsetProduct.getSceneRasterHeight()));
        return true;
    }

    @Override
    public Product openProcessedProduct() {
        return subsetProduct;
    }

    @Override
    public void saveProcessedProducts(ProgressMonitor pm) throws IOException {
        saveTargetProduct(subsetProduct, pm);
    }

    @Override
    public Path getOutputProductPath() throws IOException {
        return getWorkOutputProductPath();
    }

    @Override
    public boolean supportsPullProcessing() {
        return true;
    }

    @Override
    public void dispose() {
        super.dispose();
        if (subsetProduct != null) {
            subsetProduct.dispose();
            subsetProduct = null;
        }
    }

    protected void saveTargetProduct(Product product, ProgressMonitor pm) throws IOException {
        if (product != null) {
            int tileHeight = DEFAULT_TILE_HEIGHT;
            Dimension preferredTileSize = product.getPreferredTileSize();
            if (preferredTileSize != null) {
                tileHeight = preferredTileSize.height;
            } else {
                product.setPreferredTileSize(product.getSceneRasterWidth(), DEFAULT_TILE_HEIGHT);
            }
            StreamingProductWriter.writeProductInSlices(getConfiguration(), pm, product, getWorkOutputProductPath(), tileHeight);
        } else {
            getLogger().warning("No 'targetProduct' set. Nothing to save.");
        }
    }

    private Path getWorkOutputProductPath() throws IOException {
        return new Path(getWorkOutputDirectoryPath(), getOutputProductFilename());
    }

    protected String getOutputProductFilename() {
        for (int i = 0; i < getInputParameters().length; i += 2) {
            if ("output".equals(getInputParameters()[i])) {
                return FileUtils.exchangeExtension(getInputParameters()[i + 1], ".seq");
            }
        }
        String inputFilename = getInputPath().getName();
        return "L2_of_" + FileUtils.exchangeExtension(inputFilename, ".seq");
    }

    protected Product createSubsetFromInput(Product product) throws IOException {
        Rectangle srcProductRect = getInputRectangle();
        getLogger().info("createSubsetFromInput: calculated inputProductRect = " + srcProductRect);
        if (srcProductRect == null ||
                (srcProductRect.width == product.getSceneRasterWidth() && srcProductRect.height == product.getSceneRasterHeight())) {
            // full region
            return product;
        }
        if (srcProductRect.isEmpty()) {
            throw new IllegalStateException("Can not create an empty subset.");
        }
        ProductTransformation productTransformation = new ProductTransformation(srcProductRect, false, false);
        setInput2OutputTransform(productTransformation.getTransform());
        
        return createSubset(product, srcProductRect);
    }

    protected Product createSubsetFromOutput(Product product) throws IOException {
        String geometryWkt = getConfiguration().get(JobConfigNames.CALVALUS_REGION_GEOMETRY);
        Geometry regionGeometry = GeometryUtils.createGeometry(geometryWkt);
        Rectangle outputProductRect = ProcessingRectangleCalculator.getGeometryAsRectangle(product, regionGeometry);
        getLogger().info("createSubsetFromOutput: calculated outputProductRect = " + outputProductRect);
        if (outputProductRect == null ||
                (outputProductRect.width == product.getSceneRasterWidth() && outputProductRect.height == product.getSceneRasterHeight())) {
            // full region
            return product;
        }
        return createSubset(product, outputProductRect);
    }

    protected Product createSubset(Product product, Rectangle subsetRect) throws IOException {

        final SubsetOp op = new SubsetOp();
        op.setSourceProduct(product);
        op.setRegion(subsetRect);
        op.setCopyMetadata(true);
        Product subsetProduct = op.getTargetProduct();
        getLogger().info(String.format("Created Subset product width = %d height = %d",
                subsetProduct.getSceneRasterWidth(),
                subsetProduct.getSceneRasterHeight()));
        CalvalusProductIO.printProductOnStdout(subsetProduct, "subset");
        return subsetProduct;
    }
}
