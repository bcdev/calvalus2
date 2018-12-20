package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.io.IOException;
import java.util.logging.Logger;

public class AvhrrFireGridDataSource extends AbstractFireGridDataSource {

    protected static final Logger LOG = CalvalusLogger.getLogger();
    private static final double EPS = 1.0E-5;

    private final Product porcProduct;
    private final Product uncProduct;
    private final Product lcProduct;
    private final Band[] lcFractionBand = new Band[1+18];
    private final int tileIndex;

    public AvhrrFireGridDataSource(Product porcProduct, Product lcProduct, Product uncProduct, int tileIndex) {
        super(1000, 7200);
        this.porcProduct = porcProduct;
        this.lcProduct = lcProduct;
        this.uncProduct = uncProduct;
        this.tileIndex = tileIndex;
        lcFractionBand[0] = lcProduct.getBand("lc_class_19");
        for (int i=1; i<19; ++i) {
            lcFractionBand[i] = lcProduct.getBand(String.format("lc_class_%02d", i));
        }
    }

    public String toString() {
        return "AvhrrFireGridDataSource(" + tileIndex + ")";
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        if (x == 0) {
            CalvalusLogger.getLogger().info("Reading data for line " + (y + 1) + "/80" + " for tile with index " + tileIndex);
        }
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        // target grid: 1440*720
        // source grid: 7200*3600
        // --> for a single target grid cell, read 5*5 source pixels
        SourceData data = new SourceData(5, 5);
        data.reset();

        AreaCalculator areaCalculator = new AreaCalculator(porcProduct.getSceneGeoCoding());
        Band pc = porcProduct.getBand("band_1");
        Band cl = uncProduct.getBand("band_1");
        //Band lc = lcProduct.getBand("band_1");

        for (int sourceY = 0; sourceY < data.height; sourceY++) {
            for (int sourceX = 0; sourceX < data.width; sourceX++) {

                int sourcePixelIndex = getPixelIndex(x, y, sourceX, sourceY, tileIndex);

                float sourcePC = getFloatPixelValue(pc, "porcentage", sourcePixelIndex);
                int targetPixelIndex = sourceY * 5 + sourceX;
                if (isValidPixel(sourcePC)) {
                    data.burnedPixels[targetPixelIndex] = sourcePC;
                }
                float sourceCL = getFloatPixelValue(cl, "confidence", sourcePixelIndex) / 100.0F;
                data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                //int sourceLC = getIntPixelValue(lc, "landcover", sourcePixelIndex);
                //data.lcClasses[targetPixelIndex] = sourceLC;
                //data.burnable[targetPixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);

                if (sourcePC >= 0) { // has data -> observed pixel
                    data.statusPixels[targetPixelIndex] = 1;
                } else if (sourcePC == -2.0f) { // fraction of burnable less than 0.2
                    data.statusPixels[targetPixelIndex] = 2;
                }

                int x1 = sourcePixelIndex % porcProduct.getSceneRasterWidth();
                int y1 = sourcePixelIndex / porcProduct.getSceneRasterWidth();
                int width = porcProduct.getSceneRasterWidth();
                int height = porcProduct.getSceneRasterHeight();
                data.areas[targetPixelIndex] = areaCalculator.calculatePixelSize(x1, y1, width, height);
            }
        }

        data.patchCount = -1;

        return data;
    }

    private boolean isValidPixel(float sourcePC) {
        return sourcePC != -1.0 && sourcePC != -2.0;
    }

    static int getPixelIndex(int targetX, int targetY, int sourceX, int sourceY, int tileIndex) {
        int SCALE = 5;
        int SOURCE_WIDTH = 7200;
        int TILE_WIDTH = 400;
        int TILES_PER_ROW = 18;

        int tileYOffset = (tileIndex / TILES_PER_ROW) * SOURCE_WIDTH * TILE_WIDTH;
        int tileXOffset = (tileIndex % TILES_PER_ROW) * TILE_WIDTH;
        int gridYOffset = targetY * SCALE * SOURCE_WIDTH;
        int gridXOffset = targetX * SCALE;
        int innerYOffset = sourceY * SOURCE_WIDTH;
        int innerXOffset = sourceX;

        return tileYOffset
                + tileXOffset
                + gridYOffset
                + gridXOffset
                + innerYOffset
                + innerXOffset;
    }

    public void readLcFraction(int x, int y, float[][] lcFraction) {
        for (int c = 0; c < 19; ++c) {
            for (int sourceY = 0; sourceY < 5; sourceY++) {
                for (int sourceX = 0; sourceX < 5; sourceX++) {
                    int sourcePixelIndex = getPixelIndex(x, y, sourceX, sourceY, tileIndex);
                    try {
                        float fraction = getFloatPixelValue(lcFractionBand[c], lcFractionBand[c].getName(), sourcePixelIndex);
                        if (fraction > 1.0 - EPS) {
                            fraction = 1.0f;
                        }
                        if (! Float.isNaN(fraction)) {
                           lcFraction[c][sourceY * 5 + sourceX] = fraction;
                        } else {
                            lcFraction[c][sourceY * 5 + sourceX] = 0.0f;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("failed reading LC at x=" + x + " y=" + y + " tile" + tileIndex, e);
                        //lcFraction[c][sourceY * 5 + sourceX] = 0.0f;
                    }
                }
            }
        }
    }
}
