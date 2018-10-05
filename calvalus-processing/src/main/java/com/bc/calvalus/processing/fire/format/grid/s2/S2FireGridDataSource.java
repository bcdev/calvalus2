package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.SubsetOp;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.logging.Logger;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private static final int DIMENSION = 5490;
    private final String tile;
    private final Product[] sourceProducts;
    private final Product[] clProducts;
    private final Product[] lcProducts;

    protected static final Logger LOG = CalvalusLogger.getLogger();

    public S2FireGridDataSource(String tile, Product sourceProducts[], Product[] clProducts, Product[] lcProducts) {
        super(-1, -1);
        this.tile = tile;
        this.sourceProducts = sourceProducts;
        this.clProducts = clProducts;
        this.lcProducts = lcProducts;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        CalvalusLogger.getLogger().warning("Reading data for pixel x=" + x + ", y=" + y);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        CalvalusLogger.getLogger().info("All products:");
        CalvalusLogger.getLogger().info(Arrays.toString(Arrays.stream(sourceProducts).map(Product::getName).toArray()));

        // e.g. tile == x210y94
        double lon0 = -180 + Integer.parseInt(tile.split("y")[0].replace("x", "")) + x * 0.25;
        double lat0 = -90 + Integer.parseInt(tile.split("y")[1].replace("y", "")) + y * 0.25;

        int totalWidth = 0;
        int totalHeight = 0;

        for (Product sourceProduct : sourceProducts) {

            Product jdSubset;
            jdSubset = getSubset(lon0, lat0, sourceProduct);

            totalWidth += jdSubset.getSceneRasterWidth();
            totalHeight += jdSubset.getSceneRasterHeight();
        }

        SourceData data = new SourceData(totalWidth, totalHeight);
        data.reset();

        int targetPixelIndex = 0;
        for (int i = 0; i < sourceProducts.length; i++) {

            Product sourceProduct = sourceProducts[i];
            Product clProduct = clProducts[i];
            Product lcProduct = lcProducts[i];

            Product jdSubset = getSubset(lon0, lat0, sourceProduct);
            Product clSubset = getSubset(lon0, lat0, clProduct);
            Product lcSubset = getSubset(lon0, lat0, lcProduct);

            Band jd = jdSubset.getBand("JD");
            Band cl = clSubset.getBand("CL");
            Band lc = lcSubset.getBand("LC");

            AreaCalculator areaCalculator = new AreaCalculator(jdSubset.getSceneGeoCoding());

            for (int lineIndex = 0; lineIndex < jdSubset.getSceneRasterHeight(); lineIndex++) {
                if (lineIndex % 250 == 0) {
                    System.out.println((lineIndex + 1) + "/" + jdSubset.getSceneRasterHeight());
                }
                int width = jdSubset.getSceneRasterWidth();

                int[] jdPixels = new int[width];
                float[] clPixels = new float[width];
                int[] lcPixels = new int[width];

                jd.readPixels(0, lineIndex, width, 1, jdPixels);
                cl.readPixels(0, lineIndex, width, 1, clPixels);
                lc.readPixels(0, lineIndex, width, 1, lcPixels);

                for (int x0 = 0; x0 < lcPixels.length; x0++) {
                    int sourceJD = jdPixels[x0];
                    float sourceCL = clPixels[x0];
                    int sourceLC = lcPixels[x0];

                    boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                    if (isValidPixel) {
                        // set burned pixel value consistently with CL value -- both if burned pixel is valid
                        data.burnedPixels[targetPixelIndex] = sourceJD;

                        sourceCL = scale(sourceCL);
                        data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                    }

//                        data.burnable[targetPixelIndex] = LcRemappingS2.isInBurnableLcClass(sourceLC);
                    data.lcClasses[targetPixelIndex] = sourceLC;
//                        if (sourceJD >= 0) { // neither no-data, nor water, nor cloud -> observed pixel
//                            data.statusPixels[targetPixelIndex] = 1;
//                        } else {
//                            data.statusPixels[targetPixelIndex] = 0;
//                        }

                    data.areas[targetPixelIndex] = areaCalculator.calculatePixelSize(x0, lineIndex, width - 1, jdSubset.getSceneRasterHeight() - 1);
                    targetPixelIndex++;
                }
            }

        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, totalWidth, totalHeight), GridFormatUtils.make2Dims(data.lcClasses, totalWidth, totalHeight));
        return data;
    }


    private Product getSubset(double lon0, double lat0, Product sourceProduct) {
        SubsetOp subsetOp = new SubsetOp();
        Geometry geometry;
        try {
            geometry = new WKTReader().read(String.format("POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))",
                    lon0, lat0,
                    lon0 + 0.25, lat0,
                    lon0 + 0.25, lat0 + 0.25,
                    lon0, lat0 + 0.25,
                    lon0, lat0));
        } catch (ParseException e) {
            throw new IllegalStateException(e);
        }

        subsetOp.setGeoRegion(geometry);
        subsetOp.setSourceProduct(sourceProduct);
        return subsetOp.getTargetProduct();
    }

    static StringBuilder getExpression(int productIndex, String s) {
        StringBuilder jdExpression = new StringBuilder();
        for (int i = 0; i < productIndex; i++) {
            final String band = s + "_" + i;
            jdExpression.append("(" + band + " > 0 and " + band + " < 997) ? " + band + " : ");
        }
        jdExpression.append(" 0");
        return jdExpression;
    }

    static int getTargetPixel(int sourcePixelX, int sourcePixelY, double minLon, double maxLon, double minLat, double maxLat, double targetLon0, double targetLat0) {
        // first: get geo position of sourcePixel

        double sourceLon = Math.abs(minLon + (maxLon - minLon) * sourcePixelX / DIMENSION);
        double sourceLat = Math.abs(minLat + (maxLat - minLat) * sourcePixelY / DIMENSION);

        // second: get target pixel position from geo position

        int targetPixelX;
        if (sourceLon == targetLon0) {
            targetPixelX = 0;
        } else {
            targetPixelX = (int) Math.round(sourcePixelX * (1.0 / Math.abs(sourceLon - targetLon0)));
        }
        if (targetPixelX >= DIMENSION) {
            targetPixelX = DIMENSION - 1;
        }

        int targetPixelY;
        if (sourceLat == targetLat0) {
            targetPixelY = 0;
        } else {
            targetPixelY = (int) Math.round(sourcePixelY * (1.0 / Math.abs(sourceLat - targetLat0)));
        }
        if (targetPixelY >= DIMENSION) {
            targetPixelY = DIMENSION - 1;
        }

        // third: reformat as pixel index

        return targetPixelY * DIMENSION + targetPixelX;
    }

    private float scale(float cl) {
        if (cl < 0.05 || cl > 1.0) {
            return 0.0F;
        } else if (cl <= 0.14) {
            return 0.5F;
        } else if (cl <= 0.23) {
            return 0.6F;
        } else if (cl <= 0.32) {
            return 0.7F;
        } else if (cl <= 0.41) {
            return 0.8F;
        } else if (cl <= 0.50) {
            return 0.9F;
        } else {
            return 1.0F;
        }
    }

    static int getProductJD(Product product) {
        String productDate = product.getName().substring(product.getName().lastIndexOf("-") + 1);// BA-T31NBJ-20160219T101925
        return LocalDate.parse(productDate, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")).get(ChronoField.DAY_OF_YEAR);
    }

}
