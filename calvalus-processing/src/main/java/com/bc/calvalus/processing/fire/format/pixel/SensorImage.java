package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.RenderedImage;
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.text.NumberFormat;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.SE;

class SensorImage extends SingleBandedOpImage implements RenderedImage {

    private final Band sourceJdBand;
    private final Band sourceLcBand;
    private final int sensorId;
    private final String month;
    private final PixelFinaliseMapper.NanHandler nanHandler;

    SensorImage(Band sourceJdBand, Band sourceLcBand, String sensorId, String month, PixelFinaliseMapper.NanHandler nanHandler) {
        super(DataBuffer.TYPE_BYTE, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.sourceLcBand = sourceLcBand;
        this.sensorId = Integer.parseInt(sensorId);
        this.month = month;
        this.nanHandler = nanHandler;
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of sensor image.");
        }
        float[] jdData = new float[destRect.width * destRect.height];
        try {
            sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(jdData));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        int[] lcData = new int[destRect.width * destRect.height];
        try {
            sourceLcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcData);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        int pixelIndex = 0;
        for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                float jdValue = jdData[pixelIndex];

                if (!LcRemapping.isInBurnableLcClass(LcRemapping.remap(lcData[pixelIndex]))) {
                    if (jdValue > 0) {
                        jdValue = 0;
                    }
                }

                if (Float.isNaN(jdValue)) {
                    jdValue = nanHandler.handleNaN(jdData, lcData, pixelIndex, destRect.width, SE, null, month).value;
                }

                if (jdValue > 0 && jdValue < 900) {
                    dest.setSample(x, y, 0, sensorId);
                }

                pixelIndex++;
            }
        }
    }
}
