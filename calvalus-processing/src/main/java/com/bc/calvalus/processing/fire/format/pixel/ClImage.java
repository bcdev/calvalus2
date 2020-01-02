package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
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

import static com.bc.calvalus.processing.fire.format.CommonUtils.checkForBurnability;

class ClImage extends SingleBandedOpImage {

    private final Band sourceClBand;
    private final Band sourceJdBand;
    private final Band lcBand;
    private final PixelFinaliseMapper.ClScaler clScaler;
    private final String sensor;

    ClImage(Band sourceClBand, Band sourceJdBand, Band lcBand, PixelFinaliseMapper.ClScaler clScaler, String sensor) {
        super(DataBuffer.TYPE_BYTE, sourceClBand.getRasterWidth(), sourceClBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceClBand = sourceClBand;
        this.sourceJdBand = sourceJdBand;
        this.lcBand = lcBand;
        this.clScaler = clScaler;
        this.sensor = sensor;
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

                float sourceCl = sourceClArray[pixelIndex];
                float targetCl = clScaler.scaleCl(sourceCl);
                int sourceLcClass = lcArray[pixelIndex];
                float jdValue = sourceJdArray[pixelIndex];

                if (sensor.equals("S2")) {
                    if (!LcRemappingS2.isInBurnableLcClass(lcArray[pixelIndex])) {
                        dest.setSample(x, y, 0, 0);
                        pixelIndex++;
                        continue;
                    }
                } else {
                    if (!LcRemapping.isInBurnableLcClass(lcArray[pixelIndex])) {
                        dest.setSample(x, y, 0, 0);
                        pixelIndex++;
                        continue;
                    }
                }

                if (Float.isNaN(jdValue) || jdValue == 999) {
                    PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, false, sensor);
                    if (positionAndValue.newPixelIndex != pixelIndex) {
                        // valid neighbour has been found, use it
                        targetCl = sourceClArray[positionAndValue.newPixelIndex];
                        targetCl = clScaler.scaleCl(targetCl);
                        jdValue = sourceJdArray[positionAndValue.newPixelIndex];
                        sourceLcClass = lcArray[positionAndValue.newPixelIndex];
                    } else {
                        // no valid neighbour: use JD will be -1 or -2, so set 0
                        targetCl = 0;
                    }
                }

//                boolean notCloudy = jdValue != 998 && jdValue != -1.0;
                jdValue = checkForBurnability(jdValue, sourceLcClass, sensor);

                if (jdValue < 0 || jdValue > 900) {
                    dest.setSample(x, y, 0, 0.0F);
                    pixelIndex++;
                    continue;
                }

                if (sensor.equals("MODIS")) {
                    if (targetCl == 0.0F && jdValue != -1 && jdValue != -2 && !Float.isNaN(jdValue)) {
                        // should not come here, but does as algorithm is not 100% perfect
                        targetCl = 1.0F;
                    }
                }

                dest.setSample(x, y, 0, (int) targetCl);
                pixelIndex++;
            }
        }
    }
}
