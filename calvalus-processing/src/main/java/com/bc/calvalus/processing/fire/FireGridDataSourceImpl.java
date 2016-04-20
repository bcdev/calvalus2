package com.bc.calvalus.processing.fire;

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

import java.awt.Rectangle;
import java.io.IOException;
import java.util.Map;

/**
 * Offers the standard implementation for reading pixels of arbitrary source rectangles
 *
 * @author thomas
 */
public class FireGridDataSourceImpl implements FireGridMapper.FireGridDataSource {

    private final Product centerSourceProduct;
    private final Map<FireGridMapper.Position, Product> neighbourProducts;

    public FireGridDataSourceImpl(Product centerSourceProduct, Map<FireGridMapper.Position, Product> neighbourProducts) {
        this.centerSourceProduct = centerSourceProduct;
        this.neighbourProducts = neighbourProducts;
    }

    @Override
    public void readPixels(Rectangle sourceRect, int[] pixels) throws IOException {
        final int width = centerSourceProduct.getSceneRasterWidth();
        final int height = centerSourceProduct.getSceneRasterHeight();

        final boolean top = sourceRect.y < 0;
        final boolean bottom = sourceRect.y + sourceRect.height > height;
        final boolean centerY = sourceRect.y > 0 && sourceRect.y + sourceRect.height <= height;

        final boolean left = sourceRect.x < 0;
        final boolean right = sourceRect.x + sourceRect.width > centerSourceProduct.getSceneRasterWidth();
        final boolean centerX = sourceRect.x > 0 && sourceRect.x + sourceRect.width <= centerSourceProduct.getSceneRasterWidth();

        final boolean isFullyInCenter = !left && !top && !right && !bottom;
        if (isFullyInCenter) {
            centerSourceProduct.getBand("band_1").readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, pixels);
        }
        if (left && top) {
            int[] topLeftPixels = getBand(FireGridMapper.Position.TOP_LEFT).readPixels(sourceRect.x + width, sourceRect.y + height, Math.abs(sourceRect.x), Math.abs(sourceRect.y), (int[]) null);
            int[] topCenterPixels = getBand(FireGridMapper.Position.TOP_CENTER).readPixels(0, sourceRect.y + height, sourceRect.width - Math.abs(sourceRect.x), Math.abs(sourceRect.y), (int[]) null);
            int[] centerLeftPixels = getBand(FireGridMapper.Position.CENTER_LEFT).readPixels(sourceRect.x + width, 0, Math.abs(sourceRect.x), sourceRect.height - Math.abs(sourceRect.y), (int[]) null);
            int[] centerPixels = centerSourceProduct.getBand("band_1").readPixels(0, 0, sourceRect.width - Math.abs(sourceRect.x), sourceRect.height - Math.abs(sourceRect.y), (int[]) null);
            System.arraycopy(topLeftPixels, 0, pixels, 0, topLeftPixels.length);
            System.arraycopy(topCenterPixels, 0, pixels, topLeftPixels.length, topCenterPixels.length);
            System.arraycopy(centerLeftPixels, 0, pixels, topLeftPixels.length + topCenterPixels.length, centerLeftPixels.length);
            System.arraycopy(centerPixels, 0, pixels, topLeftPixels.length + topCenterPixels.length + centerLeftPixels.length, centerPixels.length);
        } else if (left && centerY) {
            int[] centerLeftPixels = getBand(FireGridMapper.Position.CENTER_LEFT).readPixels(sourceRect.x + width, 0, Math.abs(sourceRect.x), sourceRect.height - Math.abs(sourceRect.y), (int[]) null);
            int[] centerPixels = centerSourceProduct.getBand("band_1").readPixels(0, 0, sourceRect.width - Math.abs(sourceRect.x), sourceRect.height - Math.abs(sourceRect.y), (int[]) null);
            System.arraycopy(centerLeftPixels, 0, pixels, 0, centerLeftPixels.length);
            System.arraycopy(centerPixels, 0, pixels, centerLeftPixels.length, centerPixels.length);
        }
    }

    private Band getBand(FireGridMapper.Position position) {
        return neighbourProducts.get(position).getBand("band_1");
    }
}
