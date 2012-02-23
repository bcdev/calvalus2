package com.bc.calvalus.processing.mosaic;

import com.sun.media.imageio.plugins.tiff.TIFFField;
import com.sun.media.imageio.plugins.tiff.TIFFTag;
import com.sun.media.imageioimpl.plugins.tiff.TIFFIFD;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageMetadata;
import com.sun.media.imageioimpl.plugins.tiff.TIFFImageReader;
import org.apache.hadoop.http.HtmlQuoting;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.DOMBuilder;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.awt.image.Raster;
import java.awt.image.RenderedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

/**
 * The cloud detection from UCL.
 *
 * @author MarcoZ
 */
public class UclCloudDetection {

    private static final float CLOUD_UNCERTAINTY_THRESHOLD = -0.1f;

    private static final int HUE = 0;
    private static final int SAT = 1;
    private static final int VAL = 2;

    private static final String CLOUD_SCATTER_FILE = "MER_FSG_SDR.SV_CLOUD.scatter_logsum.tif";
    private static final String LAND_SCATTER_FILE = "MER_FSG_SDR.HS_LAND.scatter_logsum.tif";

    final ScatterData cloudScatterData;
    final ScatterData landScatterData;

    UclCloudDetection(ScatterData cloud, ScatterData land) {
        this.cloudScatterData = cloud;
        this.landScatterData = land;
    }

    public static UclCloudDetection create() throws IOException {
        float[] cloudXIndices = new float[]{-0.1f, -0.09f, -0.08f, -0.07f, -0.06f, -0.05f, -0.04f, -0.03f, -0.02f, -0.01f, -1.49012e-08f,
                0.00999998f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.1f, 0.11f, 0.12f, 0.13f,
                .14f, 0.15f, 0.16f, 0.17f, 0.18f, 0.19f, 0.2f, 0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f, 0.27f,
                0.28f, 0.29f, 0.3f, 0.31f, 0.32f, 0.33f, 0.34f, 0.35f, 0.36f, 0.37f, 0.38f, 0.39f, 0.4f, 0.41f,
                0.42f, 0.43f, 0.44f, 0.45f, 0.46f, 0.47f, 0.48f, 0.49f, 0.5f, 0.51f, 0.52f, 0.53f, 0.54f, 0.55f,
                0.56f, 0.57f, 0.58f, 0.59f, 0.6f, 0.61f, 0.62f, 0.63f, 0.64f, 0.65f, 0.66f, 0.67f, 0.68f, 0.69f,
                0.7f, 0.71f, 0.72f, 0.73f, 0.74f, 0.75f, 0.76f, 0.77f, 0.78f, 0.79f, 0.8f, 0.81f, 0.82f, 0.83f,
                0.839999f, 0.849999f, 0.859999f, 0.869999f, 0.879999f, 0.889999f, 0.899999f, 0.909999f, 0.919999f,
                0.929999f, 0.939999f, 0.949999f, 0.959999f, 0.969999f, 0.979999f, 0.989999f, 0.999999f, 1.01f,
                1.02f, 1.03f, 1.04f, 1.05f, 1.06f, 1.07f, 1.08f, 1.09f, 1.1f};
        float[] cloudYIndices = new float[]{0f, 0.01f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.1f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f, 0.17f, 0.18f, 0.19f, 0.2f, 0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f, 0.27f, 0.28f, 0.29f, 0.3f, 0.31f, 0.32f, 0.33f, 0.34f, 0.35f, 0.36f, 0.37f, 0.38f, 0.39f, 0.4f, 0.41f, 0.42f, 0.43f, 0.44f, 0.45f, 0.46f, 0.47f, 0.48f, 0.49f, 0.5f, 0.51f, 0.52f, 0.53f, 0.54f, 0.55f, 0.56f, 0.57f, 0.58f, 0.59f, 0.6f, 0.61f, 0.62f, 0.63f, 0.64f, 0.65f, 0.66f, 0.67f, 0.68f, 0.69f, 0.7f, 0.71f, 0.72f, 0.73f, 0.74f, 0.75f, 0.76f, 0.77f, 0.78f, 0.79f, 0.8f, 0.81f, 0.82f, 0.83f, 0.839999f, 0.849999f, 0.859999f, 0.869999f, 0.879999f, 0.889999f, 0.899999f, 0.909999f, 0.919999f, 0.929999f, 0.939999f, 0.949999f, 0.959999f, 0.969999f, 0.979999f, 0.989999f, 0.999999f, 1.01f, 1.02f, 1.03f, 1.04f, 1.05f, 1.06f, 1.07f, 1.08f, 1.09f, 1.1f, 1.11f, 1.12f, 1.13f, 1.14f, 1.15f, 1.16f, 1.17f, 1.18f, 1.19f, 1.2f, 1.21f, 1.22f, 1.23f, 1.24f, 1.25f, 1.26f, 1.27f, 1.28f, 1.29f, 1.3f, 1.31f, 1.32f, 1.33f, 1.34f, 1.35f, 1.36f, 1.37f, 1.38f, 1.39f, 1.4f, 1.41f, 1.42f, 1.43f, 1.44f, 1.45f, 1.46f, 1.47f, 1.48f, 1.49f, 1.5f, 1.51f, 1.52f, 1.53f, 1.54f, 1.55f, 1.56f, 1.57f, 1.58f, 1.59f, 1.6f, 1.61f, 1.62f, 1.63f, 1.64f, 1.65f, 1.66f, 1.67f, 1.68f, 1.69f, 1.7f, 1.71f, 1.72f, 1.73f, 1.74f, 1.75f, 1.76f, 1.77f, 1.78f, 1.79f, 1.8f, 1.81f, 1.82f, 1.83f, 1.84f, 1.85f, 1.86f, 1.87f, 1.88f, 1.89f, 1.9f, 1.91f, 1.92f, 1.93f, 1.94f, 1.95f, 1.96f, 1.97f, 1.98f, 1.99f, 2f, 2.01f, 2.02f, 2.03f, 2.04f, 2.05f, 2.06f, 2.07f, 2.08f, 2.09f, 2.1f, 2.11f, 2.12f, 2.13f, 2.14f, 2.15f, 2.16f, 2.17f, 2.18f, 2.19f, 2.2f, 2.21f, 2.22f, 2.23f, 2.24f, 2.25f, 2.26f, 2.27f, 2.28f, 2.29f, 2.3f, 2.31f, 2.32f, 2.33f, 2.34f, 2.35f, 2.36f, 2.37f, 2.38f, 2.39f, 2.4f, 2.41f, 2.42f, 2.43f, 2.44f, 2.45f, 2.46f, 2.47f, 2.48f, 2.49f, 2.5f, 2.51f, 2.52f, 2.53f, 2.54f, 2.55f, 2.56f, 2.57f, 2.58f, 2.59f, 2.6f, 2.61f, 2.62f, 2.63f, 2.64f, 2.65f, 2.66f, 2.67f, 2.68f, 2.69f, 2.7f, 2.71f, 2.72f, 2.73f, 2.74f, 2.75f, 2.76f, 2.77f, 2.78f, 2.79f, 2.8f, 2.81f, 2.82f, 2.83f, 2.84f, 2.85f, 2.86f, 2.87f, 2.88f, 2.89f, 2.9f, 2.91f, 2.92f, 2.93f, 2.94f, 2.95f, 2.96f, 2.97f, 2.98f, 2.99f, 3f};

        float[] landXIndices = new float[]{0f, 1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f, 16f, 17f, 18f, 19f, 20f, 21f, 22f, 23f, 24f, 25f, 26f, 27f, 28f, 29f, 30f, 31f, 32f, 33f, 34f, 35f, 36f, 37f, 38f, 39f, 40f, 41f, 42f, 43f, 44f, 45f, 46f, 47f, 48f, 49f, 50f, 51f, 52f, 53f, 54f, 55f, 56f, 57f, 58f, 59f, 60f, 61f, 62f, 63f, 64f, 65f, 66f, 67f, 68f, 69f, 70f, 71f, 72f, 73f, 74f, 75f, 76f, 77f, 78f, 79f, 80f, 81f, 82f, 83f, 84f, 85f, 86f, 87f, 88f, 89f, 90f, 91f, 92f, 93f, 94f, 95f, 96f, 97f, 98f, 99f, 100f, 101f, 102f, 103f, 104f, 105f, 106f, 107f, 108f, 109f, 110f, 111f, 112f, 113f, 114f, 115f, 116f, 117f, 118f, 119f, 120f, 121f, 122f, 123f, 124f, 125f, 126f, 127f, 128f, 129f, 130f, 131f, 132f, 133f, 134f, 135f, 136f, 137f, 138f, 139f, 140f, 141f, 142f, 143f, 144f, 145f, 146f, 147f, 148f, 149f, 150f, 151f, 152f, 153f, 154f, 155f, 156f, 157f, 158f, 159f, 160f, 161f, 162f, 163f, 164f, 165f, 166f, 167f, 168f, 169f, 170f, 171f, 172f, 173f, 174f, 175f, 176f, 177f, 178f, 179f, 180f, 181f, 182f, 183f, 184f, 185f, 186f, 187f, 188f, 189f, 190f, 191f, 192f, 193f, 194f, 195f, 196f, 197f, 198f, 199f, 200f, 201f, 202f, 203f, 204f, 205f, 206f, 207f, 208f, 209f, 210f, 211f, 212f, 213f, 214f, 215f, 216f, 217f, 218f, 219f, 220f, 221f, 222f, 223f, 224f, 225f, 226f, 227f, 228f, 229f, 230f, 231f, 232f, 233f, 234f, 235f, 236f, 237f, 238f, 239f, 240f, 241f, 242f, 243f, 244f, 245f, 246f, 247f, 248f, 249f, 250f, 251f, 252f, 253f, 254f, 255f, 256f, 257f, 258f, 259f, 260f, 261f, 262f, 263f, 264f, 265f, 266f, 267f, 268f, 269f, 270f, 271f, 272f, 273f, 274f, 275f, 276f, 277f, 278f, 279f, 280f, 281f, 282f, 283f, 284f, 285f, 286f, 287f, 288f, 289f, 290f, 291f, 292f, 293f, 294f, 295f, 296f, 297f, 298f, 299f, 300f, 301f, 302f, 303f, 304f, 305f, 306f, 307f, 308f, 309f, 310f, 311f, 312f, 313f, 314f, 315f, 316f, 317f, 318f, 319f, 320f, 321f, 322f, 323f, 324f, 325f, 326f, 327f, 328f, 329f, 330f, 331f, 332f, 333f, 334f, 335f, 336f, 337f, 338f, 339f, 340f, 341f, 342f, 343f, 344f, 345f, 346f, 347f, 348f, 349f, 350f, 351f, 352f, 353f, 354f, 355f, 356f, 357f, 358f, 359f, 360f};
        float[] landYIndices = new float[]{0f, 0.01f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.1f, 0.11f, 0.12f, 0.13f, 0.14f, 0.15f, 0.16f, 0.17f, 0.18f, 0.19f, 0.2f, 0.21f, 0.22f, 0.23f, 0.24f, 0.25f, 0.26f, 0.27f, 0.28f, 0.29f, 0.3f, 0.31f, 0.32f, 0.33f, 0.34f, 0.35f, 0.36f, 0.37f, 0.38f, 0.39f, 0.4f, 0.41f, 0.42f, 0.43f, 0.44f, 0.45f, 0.46f, 0.47f, 0.48f, 0.49f, 0.5f, 0.51f, 0.52f, 0.53f, 0.54f, 0.55f, 0.56f, 0.57f, 0.58f, 0.59f, 0.6f, 0.61f, 0.62f, 0.63f, 0.64f, 0.65f, 0.66f, 0.67f, 0.68f, 0.69f, 0.7f, 0.71f, 0.72f, 0.73f, 0.74f, 0.75f, 0.76f, 0.77f, 0.78f, 0.79f, 0.8f, 0.81f, 0.82f, 0.83f, 0.839999f, 0.849999f, 0.859999f, 0.869999f, 0.879999f, 0.889999f, 0.899999f, 0.909999f, 0.919999f, 0.929999f, 0.939999f, 0.949999f, 0.959999f, 0.969999f, 0.979999f, 0.989999f, 1f};
        ScatterData cloud = new ScatterData(SAT, VAL, cloudXIndices, cloudYIndices, readScatterRaster(CLOUD_SCATTER_FILE));
        ScatterData land = new ScatterData(HUE, SAT, landXIndices, landYIndices, readScatterRaster(LAND_SCATTER_FILE));

        return new UclCloudDetection(cloud, land);
    }

    private static Raster readScatterRaster(String scatterFile) throws IOException {
        InputStream inputStream = UclCloudDetection.class.getResourceAsStream(scatterFile);
        ImageInputStream imageInputStream = ImageIO.createImageInputStream(inputStream);
        TIFFImageReader imageReader = open(imageInputStream);
        RenderedImage image = imageReader.readAsRenderedImage(0, null);
        return image.getData();
    }

    //TODO handle metadata instead of having it in static fields
    private static void handleMetadata(TIFFImageMetadata imageMetadata) {
        final TIFFIFD tiffifd = imageMetadata.getRootIFD();
        final TIFFField[] tiffFields = tiffifd.getTIFFFields();
        for (TIFFField tiffField : tiffFields) {
            final TIFFTag tiffTag = tiffField.getTag();
            final int dataCount = tiffField.getCount();
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < dataCount; i++) {
                if (tiffTag.hasValueNames()) {
                    sb.append(tiffTag.getValueName(tiffField.getAsInt(i)));
                } else {
                    sb.append(tiffField.getValueAsString(i));
                }
                if (i + 1 < dataCount) {
                    sb.append(", ");
                }
            }
            String content = sb.toString();
            if (content.startsWith("<GDALMetadata>")) {
                try {
                    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                    final DocumentBuilder builder = factory.newDocumentBuilder();
                    InputStream is = new ByteArrayInputStream(content.getBytes());
                    final Document document = new DOMBuilder().build(builder.parse(is));
                    Element rootElement = document.getRootElement();
                    List<Element> children = rootElement.getChildren();
                    for (Element child : children) {
                        String unquoted = HtmlQuoting.unquoteHtmlChars(child.getValue());
                        System.out.println("unquoted = " + unquoted);
                        StringReader stringReader = new StringReader(unquoted);
                        JsonFactory jsonFactory = new JsonFactory();
                        JsonParser jsonParser = jsonFactory.createJsonParser(stringReader);
                        jsonParser.nextToken();
                        while (jsonParser.hasCurrentToken()) {
                            String currentName = jsonParser.getCurrentName();
                            System.out.println("currentName = " + currentName);
                            JsonToken currentToken = jsonParser.getCurrentToken();
                            System.out.println("currentToken = " + currentToken);
                            jsonParser.nextToken();
                        }
                        String attributeName = child.getAttribute("name").getValue();
                        System.out.println("name1 = " + attributeName);
                    }
                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

            }
        }
    }

    private static TIFFImageReader open(ImageInputStream stream) throws IOException {
        Iterator<ImageReader> imageReaders = ImageIO.getImageReaders(stream);
        TIFFImageReader imageReader = null;
        while (imageReaders.hasNext()) {
            final ImageReader reader = imageReaders.next();
            System.out.println("reader = " + reader);
            if (reader instanceof TIFFImageReader) {
                imageReader = (TIFFImageReader) reader;
                break;
            }
        }
        if (imageReader == null) {
            throw new IOException("GeoTiff imageReader not found");
        }
        imageReader.setInput(stream);
        return imageReader;
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

    static int findIndex(float[] scatterIndex, float value) {
        for (int index = 0; index < (scatterIndex.length - 1); index++) {
            if (value >= scatterIndex[index] && value < scatterIndex[index + 1]) {
                return index;
            }
        }
        return -1;
    }

    static class ScatterData {
        private final int scatterBandX;
        private final int scatterBandY;
        private final float[] scatterIndexX;
        private final float[] scatterIndexY;
        private final Raster scatterData;

        private ScatterData(int scatterBandX, int scatterBandY, float[] scatterIndexX, float[] scatterIndexY, Raster scatterData) {
            this.scatterBandX = scatterBandX;
            this.scatterBandY = scatterBandY;
            this.scatterIndexX = scatterIndexX;
            this.scatterIndexY = scatterIndexY;
            this.scatterData = scatterData;
        }

        public float getCoefficient(float[] hsv) {
            int indexX = findIndex(scatterIndexX, hsv[scatterBandX]);
            int indexY = findIndex(scatterIndexY, hsv[scatterBandY]);
            if (indexX == -1 || indexY == -1) {
                return Float.NaN;
            }
            return scatterData.getSampleFloat(indexX, indexY, 0);
        }
    }
}
