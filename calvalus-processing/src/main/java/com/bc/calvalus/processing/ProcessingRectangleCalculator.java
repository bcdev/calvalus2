package com.bc.calvalus.processing;

import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.mapreduce.InputSplit;
import org.esa.beam.framework.datamodel.Product;
import org.esa.beam.gpf.operators.standard.SubsetOp;

import java.awt.Rectangle;
import java.io.IOException;

/**
 * Computes the rectangle of fo the input product that should be processed.
 */
abstract class ProcessingRectangleCalculator {

    private final Geometry regionGeometry;
    private final Geometry roiGeometry;
    private final InputSplit inputSplit;

    public ProcessingRectangleCalculator(Geometry regionGeometry, Geometry roiGeometry, InputSplit inputSplit) {
        this.regionGeometry = regionGeometry;
        this.roiGeometry = roiGeometry;
        this.inputSplit = inputSplit;
    }

    abstract Product getProduct() throws IOException;

    /**
     * Computes the intersection between the input product and the given geometries.
     * If there is no intersection an empty rectangle will be returned.
     * If the whole product should be processed, {@code null} will be returned.
     * The pixel region will also take information
     * from the {@code ProductSplit} based on an inventory into account.
     *
     * @return The intersection, or {@code null} if no restriction is given.
     * @throws IOException
     */
    public Rectangle computeRect() throws IOException {
        Geometry combinedGeometry = getCombinedGeometry();
        Rectangle pixelRegion = geometryToPixelRegion(combinedGeometry);
        if ((pixelRegion == null || !pixelRegion.isEmpty()) && inputSplit instanceof ProductSplit) {
            pixelRegion = applyProdcutSplit(pixelRegion);
        }
        return pixelRegion;
    }

    Geometry getCombinedGeometry() {
        if (regionGeometry != null && roiGeometry != null)
            return regionGeometry.intersection(roiGeometry);
        else if (regionGeometry != null) {
            return regionGeometry;
        } else if (roiGeometry != null) {
            return roiGeometry;
        } else {
            return null;
        }
    }

    /**
     * adjust region to start/stop line
     */
    Rectangle applyProdcutSplit(Rectangle pixelRegion) throws IOException {
        ProductSplit productSplit = (ProductSplit) inputSplit;
        final int processStart = productSplit.getProcessStartLine();
        final int processLength = productSplit.getProcessLength();
        if (processLength > 0) {
            Product product = getProduct();
            if (pixelRegion == null) {
                pixelRegion = new Rectangle(product.getSceneRasterWidth(), product.getSceneRasterHeight());
            }
            final int width = product.getSceneRasterWidth();
            pixelRegion = pixelRegion.intersection(new Rectangle(0, processStart, width, processLength));
        }
        return pixelRegion;
    }

    Rectangle geometryToPixelRegion(Geometry regionGeometry) {
        if (!(regionGeometry == null || regionGeometry.isEmpty() || isGlobalCoverageGeometry(regionGeometry))) {
            try {
                return SubsetOp.computePixelRegion(getProduct(), regionGeometry, 1);
            } catch (Exception e) {
                // Computation of pixel region could fail (JTS Exception), if the geo-coding of the product is messed up
                // in this case ignore this product
                return new Rectangle();
            }
        }
        return null;
    }

    static boolean isGlobalCoverageGeometry(Geometry geometry) {
        Envelope envelopeInternal = geometry.getEnvelopeInternal();
        return eq(envelopeInternal.getMinX(), -180.0, 1E-8)
                && eq(envelopeInternal.getMaxX(), 180.0, 1E-8)
                && eq(envelopeInternal.getMinY(), -90.0, 1E-8)
                && eq(envelopeInternal.getMaxY(), 90.0, 1E-8);
    }

    private static boolean eq(double x1, double x2, double eps) {
        double delta = x1 - x2;
        return delta > 0 ? delta < eps : -delta < eps;
    }

}
