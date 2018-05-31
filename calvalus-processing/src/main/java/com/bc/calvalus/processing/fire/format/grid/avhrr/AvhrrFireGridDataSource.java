package com.bc.calvalus.processing.fire.format.grid.avhrr;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.logging.Logger;

public class AvhrrFireGridDataSource extends AbstractFireGridDataSource {

    protected static final Logger LOG = CalvalusLogger.getLogger();

    private final Product datesProduct;
    private final Product lcProduct;
    private final Product uncProduct;
    private final int tileIndex;

    public AvhrrFireGridDataSource(Product datesProduct, Product lcProduct, Product uncProduct, int tileIndex) {
        super(1000, 7200);
        this.datesProduct = datesProduct;
        this.lcProduct = lcProduct;
        this.uncProduct = uncProduct;
        this.tileIndex = tileIndex;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        if (x == 0) {
            CalvalusLogger.getLogger().warning("Reading data for line " + y + " for tile with index " + tileIndex);
        }
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        // target grid: 1440*720
        // source grid: 7200*3600
        // --> for a single target grid cell, read 5*5 source pixels
        SourceData data = new SourceData(5, 5);
        data.reset();

        AreaCalculator areaCalculator = new AreaCalculator(datesProduct.getSceneGeoCoding());
        Band jd = datesProduct.getBand("band_1");
        Band cl = uncProduct.getBand("band_1");
        Band lc = lcProduct.getBand("lccs_class");

        for (int sourceY = 0; sourceY < data.height; sourceY++) {
            for (int sourceX = 0; sourceX < data.height; sourceX++) {

                int sourcePixelIndex = getPixelIndex(x, y, sourceX, sourceY, tileIndex);
                int sourceJD = (int) getFloatPixelValue(jd, "", sourcePixelIndex);
                boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                int targetPixelIndex = sourceY * 5 + sourceX;
                if (isValidPixel) {
                    float sourceCL = getFloatPixelValue(cl, "", sourcePixelIndex);
                    sourceCL = scale(sourceCL);
                    data.burnedPixels[targetPixelIndex] = sourceJD;
                    data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                }
                int sourceLC = getIntPixelValue(lc, "", sourcePixelIndex);
                data.burnable[targetPixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                data.lcClasses[targetPixelIndex] = sourceLC;

                if (sourceJD != -1) { // neither no-data, nor water, nor cloud -> observed pixel
                    data.statusPixels[targetPixelIndex] = 1;
                }

                data.areas[targetPixelIndex] = areaCalculator.calculatePixelSize(sourceX, sourceY, 4, 4);
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 5, 5));

        return data;
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

    private float scale(float cl) {
        if (cl < 0.01) {
            return 0F;
        } else if (cl < 0.02) {
            return 0.1F;
        } else if (cl < 0.03) {
            return 0.2F;
        } else if (cl < 0.04) {
            return 0.3F;
        } else if (cl < 0.05) {
            return 0.4F;
        } else if (cl <= 0.14) {
            return 0.5F;
        } else if (cl <= 0.23) {
            return 0.6F;
        } else if (cl <= 0.32) {
            return 0.7F;
        } else if (cl <= 0.41) {
            return 0.8F;
        } else if (cl <= 0.50) {
            return 0.9F;
        } else {
            return 1.0F;
        }
    }

    static int getProductJD(Product product) {
        String productDate = product.getName().substring(product.getName().lastIndexOf("-") + 1);// BA-T31NBJ-20160219T101925
        return LocalDate.parse(productDate, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")).get(ChronoField.DAY_OF_YEAR);
    }

}
