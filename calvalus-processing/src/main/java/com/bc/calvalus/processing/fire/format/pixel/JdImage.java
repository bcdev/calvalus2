package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;

class JdImage extends SingleBandedOpImage {

    private final Band sourceJdBand;
    private final Band lcBand;
    private final String sensor;
    private final Band sourceClBand;

    JdImage(Band sourceJdBand, Band sourceClBand, Band lcBand, String sensor) {
        super(DataBuffer.TYPE_SHORT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.sourceClBand = sourceClBand;
        this.lcBand = lcBand;
        this.sensor = sensor;
    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of JD image.");
        }
        float[] sourceJdArray = new float[destRect.width * destRect.height];
        float[] sourceClArray = new float[destRect.width * destRect.height];
        int[] lcArray = new int[destRect.width * destRect.height];
        try {
            lcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcArray);
            sourceClBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, sourceClArray);
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
                float sourceCl = sourceClArray[pixelIndex];
                int sourceLcClass = lcArray[pixelIndex];

                boolean notCloudy = sourceJd != 998 && sourceJd != -1.0;
                sourceJd = checkForBurnability(sourceJd, sourceLcClass, notCloudy, sensor);

                if (Float.isNaN(sourceJd) || sourceJd == 999) {
                    PixelFinaliseMapper.PositionAndValue neighbourValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, sensor);
                    sourceJd = neighbourValue.value;
                    sourceCl = sourceClArray[neighbourValue.newPixelIndex];
                    sourceLcClass = lcArray[neighbourValue.newPixelIndex];
                }

                sourceJd = checkForBurnability(sourceJd, sourceLcClass, notCloudy, sensor);

                int targetJd;
                if (sourceJd < 900) {
                    targetJd = (int) sourceJd;
                } else {
                    targetJd = -1;
                }

                if (sourceCl < 0.05F && targetJd != -1 && targetJd != -2) {
                    targetJd = 0;
                }

                dest.setSample(x, y, 0, targetJd);

                pixelIndex++;
            }
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("pixelposes")))) {
            br.lines().forEach(
                    l -> {
                        int x = Integer.parseInt(l.split(" ")[0]);
                        int y = Integer.parseInt(l.split(" ")[1]);
                        if (destRect.contains(x, y)) {
                            if (dest.getSample(x, y, 0) != -1) {
                                dest.setSample(x, y, 0, -2);
                            }
                        }
                    }
            );
        } catch (IOException e) {
            throw new IllegalStateException("Programming error, must not come here", e);
        }

    }

    private static float checkForBurnability(float sourceJd, int sourceLcClass, boolean notCloudy, String sensor) {
        switch (sensor) {
            case "S2":
                if (!LcRemappingS2.isInBurnableLcClass(sourceLcClass) && notCloudy) {
                    return -2;
                } else {
                    return sourceJd;
                }
            case "MODIS":
                if (!LcRemapping.isInBurnableLcClass(sourceLcClass) && notCloudy) {
                    return -2;
                } else {
                    return sourceJd;
                }
            default:
                throw new IllegalStateException("Unknown sensor '" + sensor + "'");
        }
    }

}
