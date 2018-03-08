package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.text.NumberFormat;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.JD;

class JdImage extends SingleBandedOpImage {

    private final Band sourceJdBand;
    private final Band lcBand;
    private final PixelFinaliseMapper.NanHandler nanHandler;
    private final String month;

    JdImage(Band sourceJdBand, Band lcBand, PixelFinaliseMapper.NanHandler nanHandler, String month) {
        super(DataBuffer.TYPE_SHORT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.lcBand = lcBand;
        this.nanHandler = nanHandler;
        this.month = month;
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of JD image.");
        }
        float[] sourceJdArray = new float[destRect.width * destRect.height];
        int[] lcArray = new int[destRect.width * destRect.height];
        try {
            lcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcArray);
            sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceJdArray));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int pixelIndex = 0;
        PixelPos pixelPos = new PixelPos();
        for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                pixelPos.x = x;
                pixelPos.y = y;

                float sourceJd = sourceJdArray[pixelIndex];
                if (!LcRemapping.isInBurnableLcClass(LcRemapping.remap(lcArray[pixelIndex]))) {
                    if (sourceJd > 0) {
                        sourceJd = 0;
                    }
                }

                if (Float.isNaN(sourceJd)) {
                    GeoPos geoPos = sourceJdBand.getProduct().getSceneGeoCoding().getGeoPos(pixelPos, null);
                    sourceJd = nanHandler.handleNaN(sourceJdArray, lcArray, pixelIndex, destRect.width, JD, geoPos, month).value;
                }

                int targetJd;
                if (sourceJd < 900) {
                    targetJd = (int) sourceJd;
                } else {
                    targetJd = -1;
                }

                dest.setSample(x, y, 0, targetJd);

                pixelIndex++;
            }
        }
    }
}
