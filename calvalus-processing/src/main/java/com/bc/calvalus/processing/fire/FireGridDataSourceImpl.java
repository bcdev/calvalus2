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
    public void readPixels(Rectangle sourceRect, double[] areas, int[] pixels) throws IOException {
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
            Band band = centerSourceProduct.getBand("band_1");
            int[] readPixels = band.readPixels(sourceRect.x, sourceRect.y, sourceRect.width, sourceRect.height, pixels);
            double[] areas1 = getAreas(band, readPixels);
            System.arraycopy(areas1, 0, areas, 0, areas1.length);
            return;
        }
        if (left && top) {
            Result topLeft = getTopLeft(sourceRect, width, height);
            Result centerLeft = getCenterLeftPixels(sourceRect, width, height);
            Result topCenter = getTopCenterPixels(sourceRect, width, height);
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);

            System.arraycopy(topLeft.pixels, 0, pixels, 0, topLeft.pixels.length);
            System.arraycopy(topLeft.areas, 0, areas, 0, topLeft.areas.length);
            System.arraycopy(centerLeft.pixels, 0, pixels, topLeft.pixels.length, centerLeft.pixels.length);
            System.arraycopy(centerLeft.areas, 0, areas, topLeft.areas.length, centerLeft.areas.length);
            System.arraycopy(topCenter.pixels, 0, pixels, topLeft.pixels.length + centerLeft.pixels.length, topCenter.pixels.length);
            System.arraycopy(topCenter.areas, 0, areas, topLeft.areas.length + centerLeft.areas.length, topCenter.areas.length);
            System.arraycopy(center.pixels, 0, pixels, topLeft.pixels.length + topCenter.pixels.length + centerLeft.pixels.length, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, topLeft.pixels.length + topCenter.pixels.length + centerLeft.pixels.length, center.areas.length);
        } else if (left && centerY) {
            Result centerLeft = getCenterLeftPixels(sourceRect, width, height);
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);

            System.arraycopy(centerLeft.pixels, 0, pixels, 0, centerLeft.pixels.length);
            System.arraycopy(centerLeft.areas, 0, areas, 0, centerLeft.areas.length);
            System.arraycopy(center.pixels, 0, pixels, centerLeft.pixels.length, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, centerLeft.areas.length, center.areas.length);
        } else if (left && bottom) {
            Result centerLeft = getCenterLeftPixels(sourceRect, width, height);
            Result bottomLeft = getBottomLeftPixels(sourceRect, width, height);
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            Result bottomCenter = getBottomCenterPixels(sourceRect, width, height);

            System.arraycopy(centerLeft.pixels, 0, pixels, 0, centerLeft.pixels.length);
            System.arraycopy(centerLeft.areas, 0, areas, 0, centerLeft.areas.length);
            System.arraycopy(bottomLeft.pixels, 0, pixels, centerLeft.pixels.length, bottomLeft.pixels.length);
            System.arraycopy(bottomLeft.areas, 0, areas, centerLeft.areas.length, bottomLeft.areas.length);
            System.arraycopy(center.pixels, 0, pixels, centerLeft.pixels.length + bottomLeft.pixels.length, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, centerLeft.areas.length + bottomLeft.areas.length, center.areas.length);
            System.arraycopy(bottomCenter.pixels, 0, pixels, centerLeft.pixels.length + bottomLeft.pixels.length + center.pixels.length, bottomCenter.pixels.length);
            System.arraycopy(bottomCenter.areas, 0, areas, centerLeft.pixels.length + bottomLeft.pixels.length + center.pixels.length, bottomCenter.areas.length);
        } else if (top && centerX) {
            Result topCenter = getTopCenterPixels(sourceRect, width, height);
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);

            System.arraycopy(topCenter.pixels, 0, pixels, 0, topCenter.pixels.length);
            System.arraycopy(topCenter.areas, 0, areas, 0, topCenter.areas.length);
            System.arraycopy(center.pixels, 0, pixels, topCenter.pixels.length, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, topCenter.areas.length, center.areas.length);
        } else if (bottom && centerX) {
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            Result bottomCenter = getBottomCenterPixels(sourceRect, width, height);

            System.arraycopy(center.pixels, 0, pixels, 0, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, 0, center.areas.length);
            System.arraycopy(bottomCenter.pixels, 0, pixels, center.pixels.length, bottomCenter.pixels.length);
            System.arraycopy(bottomCenter.areas, 0, areas, center.areas.length, bottomCenter.areas.length);
        } else if (top && right) {
            Result topCenter = getTopCenterPixels(sourceRect, width, height);
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            Result topRight = getTopRightPixels(sourceRect, width, height);
            Result centerRight = getCenterRightPixels(sourceRect, width, height);

            System.arraycopy(topCenter.pixels, 0, pixels, 0, topCenter.pixels.length);
            System.arraycopy(topCenter.areas, 0, areas, 0, topCenter.areas.length);
            System.arraycopy(center.pixels, 0, pixels, topCenter.pixels.length, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, topCenter.areas.length, center.areas.length);
            System.arraycopy(topRight.pixels, 0, pixels, topCenter.pixels.length + center.pixels.length, topRight.pixels.length);
            System.arraycopy(topRight.areas, 0, areas, topCenter.areas.length + center.areas.length, topRight.areas.length);
            System.arraycopy(centerRight.pixels, 0, pixels, topCenter.pixels.length + center.pixels.length + topRight.pixels.length, centerRight.pixels.length);
            System.arraycopy(centerRight.areas, 0, areas, topCenter.areas.length + center.areas.length + topRight.areas.length, centerRight.areas.length);
        } else if (centerY && right) {
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            Result centerRight = getCenterRightPixels(sourceRect, width, height);

            System.arraycopy(center.pixels, 0, pixels, 0, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, 0, center.areas.length);
            System.arraycopy(centerRight.pixels, 0, pixels, center.pixels.length, centerRight.pixels.length);
            System.arraycopy(centerRight.areas, 0, areas, center.areas.length, centerRight.areas.length);
        } else if (bottom && right) {
            Result center = getCenterPixels(sourceRect, centerSourceProduct, width, height);
            Result bottomCenter = getBottomCenterPixels(sourceRect, width, height);
            Result centerRight = getCenterRightPixels(sourceRect, width, height);
            Result bottomRightPixels = getBottomRightPixels(sourceRect, width, height);

            System.arraycopy(center.pixels, 0, pixels, 0, center.pixels.length);
            System.arraycopy(center.areas, 0, areas, 0, center.areas.length);
            System.arraycopy(bottomCenter.pixels, 0, pixels, center.pixels.length, bottomCenter.pixels.length);
            System.arraycopy(bottomCenter.areas, 0, areas, center.areas.length, bottomCenter.areas.length);
            System.arraycopy(centerRight.pixels, 0, pixels, center.pixels.length + bottomCenter.pixels.length, centerRight.pixels.length);
            System.arraycopy(centerRight.areas, 0, areas, center.areas.length + bottomCenter.areas.length, centerRight.areas.length);
            System.arraycopy(bottomRightPixels.pixels, 0, pixels, center.pixels.length + bottomCenter.pixels.length + centerRight.pixels.length, bottomRightPixels.pixels.length);
            System.arraycopy(bottomRightPixels.areas, 0, areas, center.areas.length + bottomCenter.areas.length + centerRight.areas.length, bottomRightPixels.areas.length);
        } else {
            throw new IllegalStateException("invalid source rectangle: " + sourceRect);
        }
    }

    private Result getBottomRightPixels(Rectangle sourceRect, int width, int height) throws IOException {
        final int x = 0;
        final int y = 0;
        final int w = sourceRect.width + sourceRect.x - width;
        final int h = sourceRect.height + sourceRect.y - height;
        Band band = getBand(FireGridMapper.Position.BOTTOM_RIGHT);
        int[] pixels = band.readPixels(
                x,
                y,
                w,
                h,
                (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private Result getTopRightPixels(Rectangle sourceRect, int width, int height) throws IOException {
        Band band = getBand(FireGridMapper.Position.TOP_RIGHT);
        int[] pixels = band.readPixels(
                0,
                sourceRect.y + height,
                sourceRect.width + sourceRect.x - width,
                Math.abs(sourceRect.y),
                (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private Result getBottomLeftPixels(Rectangle sourceRect, int width, int height) throws IOException {
        Band band = getBand(FireGridMapper.Position.BOTTOM_LEFT);
        int[] pixels = band.readPixels(
                (sourceRect.x + width) % width,
                0,
                Math.abs(sourceRect.x),
                sourceRect.height + sourceRect.y - height,
                (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private Result getTopLeft(Rectangle sourceRect, int width, int height) throws IOException {
        Band band = getBand(FireGridMapper.Position.TOP_LEFT);
        int[] pixels = band.readPixels(sourceRect.x + width, sourceRect.y + height, Math.abs(sourceRect.x), Math.abs(sourceRect.y), (int[]) null);
        double[] doubles = getAreas(band, pixels);
        return new Result(doubles, pixels);
    }

    private Result getCenterRightPixels(Rectangle sourceRect, int width, int height) throws IOException {
        boolean overBottom = sourceRect.y + sourceRect.height >= height;
        int x = 0;
        int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        int w = sourceRect.width + sourceRect.x - width;
        int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        Band band = getBand(FireGridMapper.Position.CENTER_RIGHT);
        int[] pixels = band.readPixels(
                x,
                y,
                w,
                h,
                (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private Result getCenterLeftPixels(Rectangle sourceRect, int width, int height) throws IOException {
        boolean overBottom = sourceRect.y + sourceRect.height >= height;
        int x = sourceRect.x + width;
        int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        int w = Math.abs(sourceRect.x);
        int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        Band band = getBand(FireGridMapper.Position.CENTER_LEFT);
        int[] pixels = band.readPixels(x, y, w, h, (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private Result getBottomCenterPixels(Rectangle sourceRect, int width, int height) throws IOException {
        boolean overRight = sourceRect.x + sourceRect.width >= width;
        int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) : overRight ? width - sourceRect.x : sourceRect.width;
        int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        int h = sourceRect.height + sourceRect.y - height;
        Band band = getBand(FireGridMapper.Position.BOTTOM_CENTER);
        int[] pixels = band.readPixels(
                x,
                0,
                w,
                h,
                (int[]) null);
        double[] doubles = getAreas(band, pixels);
        return new Result(doubles, pixels);
    }

    private Result getTopCenterPixels(Rectangle sourceRect, int width, int height) throws IOException {
        final boolean overRight = sourceRect.x + sourceRect.width >= width;
        final int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        final int y = sourceRect.y + height;
        final int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) : overRight ? width - sourceRect.x : sourceRect.width;
        final int h = height - y;
        Band band = getBand(FireGridMapper.Position.TOP_CENTER);
        int[] pixels = band.readPixels(
                x,
                y,
                w,
                h,
                (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private static Result getCenterPixels(Rectangle sourceRect, Product centerSourceProduct, int width, int height) throws IOException {
        final boolean overBottom = sourceRect.y + sourceRect.height >= height;
        final boolean overRight = sourceRect.x + sourceRect.width >= width;
        final int x = sourceRect.x < 0 ? 0 : sourceRect.x;
        final int y = sourceRect.y < 0 ? 0 : sourceRect.y;
        final int w = sourceRect.x < 0 ? sourceRect.width - Math.abs(sourceRect.x) :
                overRight ? width - sourceRect.x: sourceRect.width;
        final int h = sourceRect.y < 0 ? sourceRect.height - Math.abs(sourceRect.y) :
                overBottom ? height - sourceRect.y : sourceRect.height;
        Band band = centerSourceProduct.getBand("band_1");

        int[] pixels = band.readPixels(x, y, w, h, (int[]) null);
        double[] areas = getAreas(band, pixels);
        return new Result(areas, pixels);
    }

    private static double[] getAreas(Band band, int[] pixels) {
        int length = pixels.length;
        double[] areas = new double[length];
        if (band.getName().equals("null")) {
            LOG.info(String.format("Skipping band %s because it is empty.", band.getName()));
            return areas;
        }
        AreaCalculator areaCalculator = new AreaCalculator(band.getGeoCoding());
        for (int i = 0; i < areas.length; i++) {
            int sourceBandX = i % band.getRasterWidth();
            int sourceBandY = i / band.getRasterWidth();
            areas[i] = areaCalculator.calculatePixelSize(sourceBandX, sourceBandY);
        }
        return areas;
    }

    private Band getBand(FireGridMapper.Position position) {
        final Product product = neighbourProducts.get(position);
        if (product == null) {
            return NULL_BAND;
        }
        return product.getBand("band_1");
    }

    private static class Result {

        double[] areas;
        int[] pixels;

        Result(double[] areas, int[] pixels) {
            this.areas = areas;
            this.pixels = pixels;
        }
    }
}
