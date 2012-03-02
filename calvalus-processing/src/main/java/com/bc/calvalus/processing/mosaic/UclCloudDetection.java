package com.bc.calvalus.processing.mosaic;

import org.esa.beam.util.geotiff.IIOUtils;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.Raster;
import java.io.IOException;
import java.io.InputStream;

/**
 * The cloud detection from UCL.
 *
 * @author MarcoZ
 */
public class UclCloudDetection {

    private static final float[] hueIndices  = new float[]{0f, 7.2f, 14.4f, 21.6f, 28.8f, 36f, 43.2f, 50.4f, 57.6f, 64.8f, 72f, 79.2f, 86.4f, 93.6f, 100.8f, 108f, 115.2f, 122.4f, 129.6f, 136.8f, 144f, 151.2f, 158.4f, 165.6f, 172.8f, 180f, 187.2f, 194.4f, 201.6f, 208.8f, 216f, 223.2f, 230.4f, 237.6f, 244.8f, 252f, 259.2f, 266.4f, 273.6f, 280.8f, 288f, 295.2f, 302.4f, 309.6f, 316.8f, 324f, 331.2f, 338.4f, 345.6f, 352.8f, 360f};
    private static final float[] satIndices  = new float[]{0f, 0.02f, 0.04f, 0.06f, 0.08f, 0.1f, 0.12f, 0.14f, 0.16f, 0.18f, 0.2f, 0.22f, 0.24f, 0.26f, 0.28f, 0.3f, 0.32f, 0.34f, 0.36f, 0.38f, 0.4f, 0.42f, 0.44f, 0.46f, 0.48f, 0.5f, 0.52f, 0.54f, 0.56f, 0.58f, 0.6f, 0.62f, 0.64f, 0.66f, 0.68f, 0.7f, 0.72f, 0.74f, 0.76f, 0.78f, 0.8f, 0.82f, 0.84f, 0.86f, 0.88f, 0.9f, 0.92f, 0.94f, 0.96f, 0.98f, 1f};
    private static final float[] valIndices  = new float[]{0f, 0.02f, 0.04f, 0.06f, 0.08f, 0.1f, 0.12f, 0.14f, 0.16f, 0.18f, 0.2f, 0.22f, 0.24f, 0.26f, 0.28f, 0.3f, 0.32f, 0.34f, 0.36f, 0.38f, 0.4f, 0.42f, 0.44f, 0.46f, 0.48f, 0.5f, 0.52f, 0.54f, 0.56f, 0.58f, 0.6f, 0.62f, 0.64f, 0.66f, 0.68f, 0.7f, 0.72f, 0.74f, 0.76f, 0.78f, 0.8f, 0.82f, 0.84f, 0.86f, 0.88f, 0.9f, 0.92f, 0.94f, 0.96f, 0.98f, 1f};

    private static final float CLOUD_UNCERTAINTY_THRESHOLD = -0.1f;

    private static final String CLOUD_SCATTER_FILE = "MER_FSG_SDR.HSV_CLOUD.scatter_percentil.tif";
    private static final String LAND_SCATTER_FILE  = "MER_FSG_SDR.HSV_LAND.scatter_percentil.tif";

    final ScatterData cloudScatterData;
    final ScatterData landScatterData;

    UclCloudDetection(ScatterData cloud, ScatterData land) {
        this.cloudScatterData = cloud;
        this.landScatterData = land;
    }

    public static UclCloudDetection create() throws IOException {
        ScatterData cloud = new ScatterData(hueIndices, satIndices, valIndices, readScatterRaster(CLOUD_SCATTER_FILE));
        ScatterData land  = new ScatterData(hueIndices, satIndices, valIndices, readScatterRaster(LAND_SCATTER_FILE));
        return new UclCloudDetection(cloud, land);
    }

    private static Raster readScatterRaster(String scatterFile) throws IOException {
        InputStream inputStream = UclCloudDetection.class.getResourceAsStream(scatterFile);
        ImageInputStream imageInputStream = ImageIO.createImageInputStream(inputStream);
        IIOImage iioImage = IIOUtils.readImage(imageInputStream);
        return iioImage.getRenderedImage().getData();
    }

    public boolean isCloud(float sdrRed, float sdrGreen, float sdrBlue) {
        float[] hsv = rgb2hsv(sdrRed, sdrGreen, sdrBlue);
        float cloudCoefficient = cloudScatterData.getCoefficient(hsv);
        float landCoefficient = landScatterData.getCoefficient(hsv);
        float probability = computeProbability(landCoefficient, cloudCoefficient);
        return probability > CLOUD_UNCERTAINTY_THRESHOLD;
    }

    public static float computeProbability(float landCof, float cloudCof) {
        final float probability;
        if (Float.isNaN(landCof) && Float.isNaN(cloudCof)) {
            probability = Float.NaN;
        } else if (Float.isNaN(landCof)) {
            probability = 1f;
        } else if (Float.isNaN(cloudCof)) {
            probability = -1f;
        } else {
            probability = (cloudCof - landCof) / Math.max(cloudCof, landCof);
        }
        return probability;
    }

    public static float[] rgb2hsv(float red, float green, float blue) {
        float hue = Float.NaN;
        float sat = Float.NaN;
        float value = Float.NaN;
        if (!Float.isNaN(red) && !Float.isNaN(green) && !Float.isNaN(blue)) {
            float maxc = Math.max(red, Math.max(green, blue));
            float minc = Math.min(red, Math.min(green, blue));
            float difc = maxc - minc;
            value = maxc;
            sat = difc / maxc;
            if ((minc != maxc) && (difc != 0.0)) {
                if ((red == maxc) && (green >= blue)) {
                    hue = (60.0f * ((green - blue) / difc)) + 0.0f;
                } else if ((red == maxc) && (green < blue)) {
                    hue = (60.0f * ((green - blue) / difc)) + 360.0f;
                } else if (green == maxc) {
                    hue = (60.0f * ((blue - red) / difc)) + 120.0f;
                } else if (blue == maxc) {
                    hue = (60.0f * ((red - green) / difc)) + 240.0f;
                } else {
                    hue = Float.NaN;
                }
            }
        }
        return new float[]{hue, sat, value};
    }

    public static void main(String[] args) throws IOException {
        UclCloudDetection uclCloudDetection = UclCloudDetection.create();
        System.out.println("cloud = " + uclCloudDetection.isCloud(0.3f, 0.4f, 0.5f));
        System.out.println("cloud = " + uclCloudDetection.isCloud(0.1f, 0.2f, 0.3f));
    }

    static class ScatterData {
        private final float[] scatterIndexX;
        private final float[] scatterIndexY;
        private final float[] scatterIndexZ;
        private final Raster scatterData;

        private ScatterData(float[] scatterIndexX, float[] scatterIndexY, float[] scatterIndexZ, Raster scatterData) {
            this.scatterIndexX = scatterIndexX;
            this.scatterIndexY = scatterIndexY;
            this.scatterIndexZ = scatterIndexZ;
            this.scatterData = scatterData;
        }

        public float getCoefficient(float[] hsv) {
            final int indexX = findIndex(scatterIndexX, hsv[0]);
            final int indexY = findIndex(scatterIndexY, hsv[1]);
            final int indexZ = findIndex(scatterIndexZ, hsv[2]);
            if (indexX == -1 || indexY == -1 || indexZ == -1) {
                return Float.NaN;
            }
            final int scatterX = indexX;
            final int scatterY = indexZ * scatterIndexZ.length + indexY;
            return scatterData.getSampleFloat(scatterX, scatterY, 0);
        }

        private static int findIndex(float[] scatterIndex, float value) {
            for (int index = 0; index < (scatterIndex.length - 1); index++) {
                if (value >= scatterIndex[index] && value <= scatterIndex[index + 1]) {
                    return index;
                }
            }
            return -1;
        }
    }
}
