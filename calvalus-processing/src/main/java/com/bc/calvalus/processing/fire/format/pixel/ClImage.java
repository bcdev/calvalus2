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
import java.awt.image.WritableRaster;
import java.io.IOException;
import java.text.NumberFormat;

class ClImage extends SingleBandedOpImage {

    private final Band sourceClBand;
    private final Band sourceJdBand;
    private final Band lcBand;
    private final PixelFinaliseMapper.ClScaler clScaler;

    ClImage(Band sourceClBand, Band sourceJdBand, Band lcBand, PixelFinaliseMapper.ClScaler clScaler) {
        super(DataBuffer.TYPE_BYTE, sourceClBand.getRasterWidth(), sourceClBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceClBand = sourceClBand;
        this.sourceJdBand = sourceJdBand;
        this.lcBand = lcBand;
        this.clScaler = clScaler;
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of CL image.");
        }
        float[] sourceClArray = new float[destRect.width * destRect.height];
        float[] sourceJdArray = new float[destRect.width * destRect.height];
        int[] lcArray = new int[destRect.width * destRect.height];
        try {
            lcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcArray);
            sourceClBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceClArray));
            sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(sourceJdArray));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int pixelIndex = 0;
        for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {

                int targetCl;
                float sourceCl = sourceClArray[pixelIndex];
                sourceCl = clScaler.scaleCl(sourceCl);
                if (sourceCl > 100) {
                    sourceCl = 100.0F;
                }
                float jdValue = sourceJdArray[pixelIndex];

                if (!LcRemapping.isInBurnableLcClass(LcRemapping.remap(lcArray[pixelIndex]))) {
                    jdValue = 0;
                }

                if (Float.isNaN(jdValue) || jdValue == 999) {
                    PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, false);
                    if (positionAndValue.newPixelIndex != pixelIndex) {
                        targetCl = (int) sourceClArray[positionAndValue.newPixelIndex];
                    } else {
                        targetCl = (int) positionAndValue.value;
                    }
                } else {
                    if (jdValue >= 0 && jdValue < 900) {
                        targetCl = (int) (sourceCl);
                    } else {
                        targetCl = 0;
                    }
                }

                dest.setSample(x, y, 0, targetCl);
                pixelIndex++;
            }
        }
    }
}
