package com.bc.calvalus.processing;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.hadoop.ProductSplit;
import com.bc.calvalus.processing.utils.GeometryUtils;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.hadoop.mapreduce.InputSplit;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Computes the rectangle of fo the input product that should be processed.
 */
public abstract class ProcessingRectangleCalculator {

    private static final Logger LOG = CalvalusLogger.getLogger();

    private final Geometry regionGeometry;
    private final Rectangle roiRectangle;
    private final InputSplit inputSplit;
    private final boolean fullSwath;

    public ProcessingRectangleCalculator(Geometry regionGeometry, Rectangle roiRectangle, InputSplit inputSplit, boolean fullSwath) {
        this.regionGeometry = regionGeometry;
        this.roiRectangle = roiRectangle;
        this.inputSplit = inputSplit;
        this.fullSwath = fullSwath;
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
        Rectangle geometryRect = getGeometryAsRectangle(getProduct(), regionGeometry);
        Rectangle productSplitRect = getProductSplitAsRectangle();

        Rectangle pixelRectangle = intersectionSafe(geometryRect, productSplitRect);
        pixelRectangle = intersectionSafe(pixelRectangle, roiRectangle);
        if (fullSwath && pixelRectangle != null) {
            final int productWidth = getProduct().getSceneRasterWidth();
            pixelRectangle = new Rectangle(0, pixelRectangle.y, productWidth, pixelRectangle.height);
        }
        return pixelRectangle;
    }

    /**
     * get rectangle from start/stop line
     */
    Rectangle getProductSplitAsRectangle() throws IOException {
        if (inputSplit instanceof ProductSplit) {
            ProductSplit productSplit = (ProductSplit) inputSplit;
            final int processStart = productSplit.getProcessStartLine();
            final int processLength = productSplit.getProcessLength();
            if (processLength > 0) {
                Product product = getProduct();
                final int width = product.getSceneRasterWidth();
                return new Rectangle(0, processStart, width, processLength);
            }
        }
        return null;
    }

    public static Rectangle getGeometryAsRectangle(Product product, Geometry regionGeometry) {
        if (!(product == null || regionGeometry == null || regionGeometry.isEmpty() || GeometryUtils.isGlobalCoverageGeometry(regionGeometry))) {
            try {
                if (product.getSceneGeoCoding() != null) {
                    LOG.info("getGeometryAsRectangle:..SubsetOp.computePixelRegion");
                    return SubsetOp.computePixelRegion(product, regionGeometry, 1);
                } else {
                    return EosRectangleCalculator.computePixelRegion(product, regionGeometry, 1);
                }
            } catch (Exception e) {
                String msg = "Exception from SubsetOp.computePixelRegion: " + e.getMessage();
                LogRecord lr = new LogRecord(Level.WARNING, msg);
                lr.setSourceClassName("ProcessingRectangleCalculator");
                lr.setSourceMethodName("getGeometryAsRectangle");
                lr.setThrown(e);
                LOG.log(lr);
                LOG.warning("ignoring product");
                // Computation of pixel region could fail (JTS Exception), if the geo-coding of the product is messed up
                // in this case ignore this product
                return new Rectangle();
            }
        }
        return null;
    }

    static Rectangle intersectionSafe(Rectangle r1, Rectangle r2) {
        if (r1 == null) {
            return r2;
        } else if (r2 == null) {
            return r1;
        } else {
            return r1.intersection(r2);
        }
    }


}
