package com.bc.calvalus.processing.fire;

import com.bc.calvalus.commons.CalvalusLogger;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Offers the standard implementation for reading pixels of arbitrary source rectangles
 *
 * @author thomas
 */
public class FireGridDataSourceImpl implements FireGridMapper.FireGridDataSource {

    private final Band NULL_BAND;
    private final Product centerSourceProduct;
    private final Map<FireGridMapper.Position, Product> neighbourProducts;

    private static final Logger LOG = CalvalusLogger.getLogger();


    public FireGridDataSourceImpl(Product centerSourceProduct, Map<FireGridMapper.Position, Product> neighbourProducts) {
        this.centerSourceProduct = centerSourceProduct;
        this.neighbourProducts = neighbourProducts;
        final int[] emptyValues = new int[centerSourceProduct.getSceneRasterWidth() * centerSourceProduct.getSceneRasterHeight()];
        Arrays.fill(emptyValues, FireGridMapper.NO_DATA);
        NULL_BAND = new Band("null", ProductData.TYPE_INT32, centerSourceProduct.getSceneRasterWidth(), centerSourceProduct.getSceneRasterHeight());
        NULL_BAND.setRasterData(new ProductData.Int(emptyValues));
    }

    @Override
    public void readPixels(Rectangle sourceRect, int[] pixels) throws IOException {
        final int width = centerSourceProduct.getSceneRasterWidth();
        final int height = centerSourceProduct.getSceneRasterHeight();

        final boolean top = sourceRect.y < 0;
        final boolean bottom = sourceRect.y + sourceRect.height > height;
        final boolean centerY = sourceRect.y >= 0 && sourceRect.y + sourceRect.height <= height;

        final boolean left = sourceRect.x < 0;
        final boolean right = sourceRect.x + sourceRect.width > centerSourceProduct.getSceneRasterWidth();
        final boolean centerX = sourceRect.x >= 0 && sourceRect.x + sourceRect.width <= centerSourceProduct.getSceneRasterWidth();

        final boolean isFullyInCenter = !left && !top && !right && !bottom;
        if (isFullyInCenter) {
            centerSourceProduct.getBand("band_1").readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, pixels);
            return;
        }
        if (left && top) {
            int[] topLeftPixels = getBand(FireGridMapper.Position.TOP_LEFT).readPixels(sourceRect.x + width, sourceRect.y + height, Math.abs(sourceRect.x), Math.abs(sourceRect.y), (int[]) null);
            int[] centerLeftPixels = getCenterLeftPixels(sourceRect, width, height);
            int[] topCenterPixels = getTopCenterPixels(sourceRect, width, height);
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            System.arraycopy(topLeftPixels, 0, pixels, 0, topLeftPixels.length);
            System.arraycopy(centerLeftPixels, 0, pixels, topLeftPixels.length, centerLeftPixels.length);
            System.arraycopy(topCenterPixels, 0, pixels, topLeftPixels.length + centerLeftPixels.length, topCenterPixels.length);
            System.arraycopy(centerPixels, 0, pixels, topLeftPixels.length + topCenterPixels.length + centerLeftPixels.length, centerPixels.length);
        } else if (left && centerY) {
            int[] centerLeftPixels = getCenterLeftPixels(sourceRect, width, height);
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            System.arraycopy(centerLeftPixels, 0, pixels, 0, centerLeftPixels.length);
            System.arraycopy(centerPixels, 0, pixels, centerLeftPixels.length, centerPixels.length);
        } else if (left && bottom) {
            int[] centerLeftPixels = getBand(FireGridMapper.Position.CENTER_LEFT).readPixels(
                    (sourceRect.x + width) % width,
                    (sourceRect.y + height) % height,
                    Math.abs(sourceRect.x),
                    height - Math.abs(sourceRect.y),
                    (int[]) null);
            int[] bottomLeftPixels = getBand(FireGridMapper.Position.BOTTOM_LEFT).readPixels(
                    (sourceRect.x + width) % width,
                    0,
                    Math.abs(sourceRect.x),
                    sourceRect.height + sourceRect.y - height,
                    (int[]) null);
            int[] centerPixels = centerSourceProduct.getBand("band_1").readPixels(
                    0,
                    sourceRect.y,
                    sourceRect.width - Math.abs(sourceRect.x),
                    height - sourceRect.y,
                    (int[]) null);
            int[] bottomCenterPixels = getBottomCenterPixels(sourceRect, width, height);
            System.arraycopy(centerLeftPixels, 0, pixels, 0, centerLeftPixels.length);
            System.arraycopy(bottomLeftPixels, 0, pixels, centerLeftPixels.length, bottomLeftPixels.length);
            System.arraycopy(centerPixels, 0, pixels, centerLeftPixels.length + bottomLeftPixels.length, centerPixels.length);
            System.arraycopy(bottomCenterPixels, 0, pixels, centerLeftPixels.length + bottomLeftPixels.length + centerPixels.length, bottomCenterPixels.length);
        } else if (top && centerX) {
            int[] topCenterPixels = getTopCenterPixels(sourceRect, width, height);
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            System.arraycopy(topCenterPixels, 0, pixels, 0, topCenterPixels.length);
            System.arraycopy(centerPixels, 0, pixels, topCenterPixels.length, centerPixels.length);
        } else if (bottom && centerX) {
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            int[] bottomCenterPixels = getBottomCenterPixels(sourceRect, width, height);
            System.arraycopy(centerPixels, 0, pixels, 0, centerPixels.length);
            System.arraycopy(bottomCenterPixels, 0, pixels, centerPixels.length, bottomCenterPixels.length);
        } else if (top && right) {
            int[] topCenterPixels = getTopCenterPixels(sourceRect, width, height);
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            int[] topRightPixels = getBand(FireGridMapper.Position.TOP_RIGHT).readPixels(
                    0,
                    sourceRect.y + height,
                    sourceRect.width + sourceRect.x - width,
                    Math.abs(sourceRect.y),
                    (int[]) null);
            int[] centerRightPixels = getCenterRightPixels(sourceRect, width, height);

            System.arraycopy(topCenterPixels, 0, pixels, 0, topCenterPixels.length);
            System.arraycopy(centerPixels, 0, pixels, topCenterPixels.length, centerPixels.length);
            System.arraycopy(topRightPixels, 0, pixels, topCenterPixels.length + centerPixels.length, topRightPixels.length);
            System.arraycopy(centerRightPixels, 0, pixels, topCenterPixels.length + centerPixels.length + topRightPixels.length, centerRightPixels.length);
        } else if (centerY && right) {
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            int[] centerRightPixels = getCenterRightPixels(sourceRect, width, height);

            System.arraycopy(centerPixels, 0, pixels, 0, centerPixels.length);
            System.arraycopy(centerRightPixels, 0, pixels, centerPixels.length, centerRightPixels.length);
        } else if (bottom && right) {
            int[] centerPixels = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            int[] bottomCenterPixels = getBottomCenterPixels(sourceRect, width, height);
            int[] centerRightPixels = getCenterRightPixels(sourceRect, width, height);
            final int x = 0;
            final int y = 0;
            final int w = sourceRect.width + sourceRect.x - width;
            final int h = sourceRect.height + sourceRect.y - height;
            int[] bottomRightPixels = getBand(FireGridMapper.Position.BOTTOM_RIGHT).readPixels(
                    x,
                    y,
                    w,
                    h,
                    (int[]) null);

            System.arraycopy(centerPixels, 0, pixels, 0, centerPixels.length);
            System.arraycopy(bottomCenterPixels, 0, pixels, centerPixels.length, bottomCenterPixels.length);
            System.arraycopy(centerRightPixels, 0, pixels, centerPixels.length + bottomCenterPixels.length, centerRightPixels.length);
            System.arraycopy(bottomRightPixels, 0, pixels, centerPixels.length + bottomCenterPixels.length + centerRightPixels.length, bottomRightPixels.length);
        } else {
            throw new IllegalStateException("invalid source rectangle: " + sourceRect);
        }
    }

    private int[] getCenterRightPixels(Rectangle sourceRect, int width, int height) throws IOException {
        final boolean overBottom = sourceRect.y + sourceRect.height >= height;
        final int x = 0;
        final int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        final int w = sourceRect.width + sourceRect.x - width;
        final int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        return getBand(FireGridMapper.Position.CENTER_RIGHT).readPixels(
                x,
                y,
                w,
                h,
                (int[]) null);
    }

    private int[] getCenterLeftPixels(Rectangle sourceRect, int width, int height) throws IOException {
        boolean overBottom = sourceRect.y + sourceRect.height >= height;
        int x = sourceRect.x + width;
        int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        int w = Math.abs(sourceRect.x);
        int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        return getBand(FireGridMapper.Position.CENTER_LEFT).readPixels(x, y, w, h, (int[]) null);
    }

    private int[] getBottomCenterPixels(Rectangle sourceRect, int width, int height) throws IOException {
        final boolean overRight = sourceRect.x + sourceRect.width >= width;
        final int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) : overRight ? width - sourceRect.x : sourceRect.width;
        final int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        final int h = sourceRect.height + sourceRect.y - height;
        return getBand(FireGridMapper.Position.BOTTOM_CENTER).readPixels(
                x,
                0,
                w,
                h,
                (int[]) null);
    }

    private int[] getTopCenterPixels(Rectangle sourceRect, int width, int height) throws IOException {
        final boolean overRight = sourceRect.x + sourceRect.width >= width;
        final int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        final int y = sourceRect.y + height;
        final int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) : overRight ? width - sourceRect.x : sourceRect.width;
        final int h = height - y;
        return getBand(FireGridMapper.Position.TOP_CENTER).readPixels(
                x,
                y,
                w,
                h,
                (int[]) null);
    }

    private static int[] getCenterPixels(Rectangle sourceRect, Product centerSourceProduct, int width, int height) throws IOException {
        final boolean overBottom = sourceRect.y + sourceRect.height >= height;
        final boolean overRight = sourceRect.x + sourceRect.width >= width;
        final int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        final int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        final int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) :
                overRight ? width - sourceRect.x: sourceRect.width;
        final int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        return centerSourceProduct.getBand("band_1").readPixels(x, y, w, h, (int[]) null);
    }

    private Band getBand(FireGridMapper.Position position) {
        final Product product = neighbourProducts.get(position);
        if (product == null) {
            return NULL_BAND;
        }
        return product.getBand("band_1");
    }
}
