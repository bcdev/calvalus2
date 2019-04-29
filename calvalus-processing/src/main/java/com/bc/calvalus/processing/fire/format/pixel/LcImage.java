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

class LcImage extends SingleBandedOpImage {

    private final Band sourceLcBand;
    private final Band sourceJdBand;
    private final Band sourceClBand;
    private final String sensor;

    LcImage(Band sourceLcBand, Band sourceJdBand, Band sourceClBand, String sensor) {
        super(DataBuffer.TYPE_BYTE, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceLcBand = sourceLcBand;
        this.sourceJdBand = sourceJdBand;
        this.sourceClBand = sourceClBand;
        this.sensor = sensor;
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of LC image.");
        }
        float[] jdArray = new float[destRect.width * destRect.height];
        float[] clArray = new float[destRect.width * destRect.height];

        try {
            sourceJdBand.readRasterData(destRect.x, destRect.y, destRect.width, destRect.height, new ProductData.Float(jdArray));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        int[] lcArray = new int[destRect.width * destRect.height];
        try {
            sourceLcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcArray);
            sourceClBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, clArray);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int pixelIndex = 0;
        for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                float jdValue = jdArray[pixelIndex];
                int lcValue = lcArray[pixelIndex];
                float clValue = clArray[pixelIndex];

                if (sensor.equals("S2")) {
                    if (!LcRemappingS2.isInBurnableLcClass(lcValue)) {
                        dest.setSample(x, y, 0, 0);
                        pixelIndex++;
                        continue;
                    }
                } else {
                    if (!LcRemapping.isInBurnableLcClass(lcValue)) {
                        dest.setSample(x, y, 0, 0);
                        pixelIndex++;
                        continue;
                    }
                }

                if (Float.isNaN(jdValue) || jdValue == 999) {
                    PixelFinaliseMapper.PositionAndValue positionAndValue = PixelFinaliseMapper.findNeighbourValue(jdArray, lcArray, pixelIndex, destRect.width, false, sensor);
                    if (positionAndValue.newPixelIndex != pixelIndex) {
                        // valid neighbour has been found, use it
                        jdValue = jdArray[positionAndValue.newPixelIndex];
                        lcValue = lcArray[pixelIndex]; // use original pixel index to be consistent with original LC image
                        clValue = clArray[positionAndValue.newPixelIndex];
                    } else {
                        // no valid neighbour: JD will be -1 or -2, so set 0
                        dest.setSample(x, y, 0, 0);
                        pixelIndex++;
                        continue;
                    }
                }

//                boolean notCloudy = jdValue != 998 && jdValue != -1.0;
                jdValue = checkForBurnability(jdValue, lcValue, sensor);

                if (jdValue <= 0 || jdValue > 900) {
                    // -> not burned for sure
                    dest.setSample(x, y, 0, 0);
                    pixelIndex++;
                    continue;
                }

                dest.setSample(x, y, 0, LcRemapping.remap(lcValue));
                pixelIndex++;

            }
        }
    }
}
