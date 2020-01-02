package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.image.SingleBandedOpImage;

import javax.media.jai.PlanarImage;
import java.awt.*;
import java.awt.image.DataBuffer;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;

class JdImage extends SingleBandedOpImage {

    private final Band sourceJdBand;
    private final Band lcBand;
    private final String sensor;
    private final String area;
    private Product maskProduct;
    private float[] maskPixels;

    JdImage(Band sourceJdBand, Band lcBand, String sensor, String area, Configuration configuration) {
        super(DataBuffer.TYPE_SHORT, sourceJdBand.getRasterWidth(), sourceJdBand.getRasterHeight(), new Dimension(PixelFinaliseMapper.TILE_SIZE, PixelFinaliseMapper.TILE_SIZE), null, ResolutionLevel.MAXRES);
        this.sourceJdBand = sourceJdBand;
        this.lcBand = lcBand;
        this.sensor = sensor;
        this.area = area;

        if ("h43v13".equals(area)
                || "h44v14".equals(area)
                || "h44v15".equals(area)
                || "h45v15".equals(area)) {
            try {
                File maskFile = new File(area + "-mask.nc");
                CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/s2-mask/" + area + "-mask.nc"), maskFile, configuration);
                maskProduct = ProductIO.readProduct(maskFile);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

    }

    @Override
    protected void computeRect(PlanarImage[] sources, WritableRaster dest, Rectangle destRect) {
        if (maskProduct != null) {
            maskPixels = new float[(destRect.width * destRect.height)];
            try {
                maskProduct.getBandAt(0).readPixels(destRect.x, destRect.y, destRect.width, destRect.height, maskPixels);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        if (destRect.x == 0 && dest.getMinY() % 22 * PixelFinaliseMapper.TILE_SIZE == 0) {
            CalvalusLogger.getLogger().info("Computed " + NumberFormat.getPercentInstance().format((float) destRect.y / (float) sourceJdBand.getRasterHeight()) + " of JD image of area '" + area + "'.");
        }
        float[] sourceJdArray = new float[destRect.width * destRect.height];
        int[] lcArray = new int[destRect.width * destRect.height];
        try {
            lcBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, lcArray);
            sourceJdBand.readPixels(destRect.x, destRect.y, destRect.width, destRect.height, sourceJdArray);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        int pixelIndex = 0;
        PixelPos pixelPos = new PixelPos();

        Polygon mask1 = new Polygon(new int[]{11170, 11170, 8942, 9888, 10141, 10087, 10277, 11147}, new int[]{20407, 27816, 27816, 25623, 24088, 23259, 21898, 20271}, 8);

        for (int y = destRect.y; y < destRect.y + destRect.height; y++) {
            for (int x = destRect.x; x < destRect.x + destRect.width; x++) {
                pixelPos.x = x;
                pixelPos.y = y;

                float sourceJd = sourceJdArray[pixelIndex];
                int sourceLcClass = lcArray[pixelIndex];

                sourceJd = CommonUtils.checkForBurnability(sourceJd, sourceLcClass, sensor);

                if (Float.isNaN(sourceJd) || sourceJd == 999) {
                    PixelFinaliseMapper.PositionAndValue neighbourValue = PixelFinaliseMapper.findNeighbourValue(sourceJdArray, lcArray, pixelIndex, destRect.width, true, sensor);
                    sourceJd = neighbourValue.value;
                    sourceLcClass = lcArray[neighbourValue.newPixelIndex];
                }

                sourceJd = CommonUtils.checkForBurnability(sourceJd, sourceLcClass, sensor);

                int targetJd;
                if (sourceJd < 900) {
                    targetJd = (int) sourceJd;
                } else {
                    targetJd = -1;
                }

                dest.setSample(x, y, 0, targetJd);

                if ("h38v20".equals(area)) {
                    Point point = new Point(x, y);
                    if (mask1.contains(point)) {
                        int sample = dest.getSample(x, y, 0);
                        if (sample != -1) {
                            dest.setSample(x, y, 0, -2);
                        }
                    }
                }

                if ("h43v13".equals(area)
                        || "h44v14".equals(area)
                        || "h44v15".equals(area)
                        || "h45v15".equals(area)) {
                    float maskPixel = maskPixels[pixelIndex];
                    if (maskPixel == 255.0F) {
                        dest.setSample(x, y, 0, -1);
                    }
                }

                pixelIndex++;
            }
        }
    }

}
