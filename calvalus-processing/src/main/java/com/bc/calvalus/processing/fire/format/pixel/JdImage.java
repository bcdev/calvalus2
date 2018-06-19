package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
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
import java.io.IOException;
import java.text.NumberFormat;

class JdImage extends SingleBandedOpImage {

    private final Band sourceJdBand;
    private final Band lcBand;
    private final Band sourceClBand;

    JdImage(Band sourceJdBand, Band sourceClBand, Band lcBand) {
        super(DataBuffer.TYPE_SHORT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.sourceClBand = sourceClBand;
        this.lcBand = lcBand;
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

                if (Float.isNaN(sourceJd) || sourceJd == 999) {
                    PixelFinaliseMapper.PositionAndValue neighbourValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true);
                    sourceJd = neighbourValue.value;
                    sourceCl = sourceClArray[neighbourValue.newPixelIndex];
                    sourceLcClass = lcArray[neighbourValue.newPixelIndex];
                }

                boolean notCloudy = sourceJd != 998 && sourceJd != -1.0;
//                if (!LcRemappingS2.isInBurnableLcClass(LcRemapping.remap(lcArray[pixelIndex])) && notCloudy) {
                if (!LcRemappingS2.isInBurnableLcClass(sourceLcClass) && notCloudy) {
                    sourceJd = -2;
                }

                int targetJd;
                if (sourceJd < 900) {
                    targetJd = (int) sourceJd;
                } else {
                    targetJd = -1;
                }

                if (sourceCl < 0.5F && targetJd != -1 && targetJd != -2) {
                    targetJd = 0;
                }

                dest.setSample(x, y, 0, targetJd);

                pixelIndex++;
            }
        }
    }
}
