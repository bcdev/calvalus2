package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.OperatorException;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.IOException;
import java.util.Arrays;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    static final double MODIS_AREA_SIZE = 53664.6683222854702276;
    private final Product[] lcProducts;
    private final String tile;
    private final Product[] sourceProducts;
    private final Product[] clProducts;

    public ModisFireGridDataSource(String tile, Product sourceProducts[], Product[] clProducts, Product[] lcProducts) {
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

        double lon0 = getLon(x, tile);
        double lat0 = getLat(y, tile);

        int totalWidth = 0;
        int totalHeight = 0;

        for (Product sourceProduct : sourceProducts) {

            Product jdSubset = getSubset(lon0, lat0, sourceProduct);
            if (jdSubset == null) {
                continue;
            }

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

            if (jdSubset == null) {
                continue;
            }

            Product clSubset = getSubset(lon0, lat0, clProduct);
            Product lcSubset = getLcSubset(jdSubset, lcProduct);

            Band jd = jdSubset.getBand("JD");
            Band cl = clSubset.getBand("CL");
            Band lc = lcSubset.getBand("band_1");

            PixelPos pixelPos = new PixelPos();
            for (int lineIndex = 0; lineIndex < jdSubset.getSceneRasterHeight(); lineIndex++) {
                pixelPos.x = 0;
                pixelPos.y = lineIndex;
                int width = jdSubset.getSceneRasterWidth();

                int[] jdPixels = new int[width];
                float[] clPixels = new float[width];
                int[] lcPixels = new int[width];

                jd.readPixels(0, lineIndex, width, 1, jdPixels);
                cl.readPixels(0, lineIndex, width, 1, clPixels);
                lc.readPixels(0, lineIndex, width, 1, lcPixels);

                for (int x0 = 0; x0 < width; x0++) {
                    int sourceJD = jdPixels[x0];
                    float sourceCL = clPixels[x0];
                    int sourceLC = lcPixels[x0];
                    data.burnable[targetPixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                    if (isValidPixel) {
                        // set burned pixel value consistently with CL value -- both if burned pixel is valid
                        data.burnedPixels[targetPixelIndex] = sourceJD;
                        data.probabilityOfBurn[targetPixelIndex] = sourceCL;
                    }

                    data.lcClasses[targetPixelIndex] = sourceLC;
                    if (sourceJD >= 0) { // neither no-data, nor water, nor cloud -> observed pixel
                        data.statusPixels[targetPixelIndex] = 1;
                    } else {
                        data.statusPixels[targetPixelIndex] = 0;
                    }

                    data.areas[targetPixelIndex] = MODIS_AREA_SIZE;
                    targetPixelIndex++;
                }
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, totalWidth, totalHeight), GridFormatUtils.make2Dims(data.burnable, totalWidth, totalHeight));
        return data;
    }

    protected static double getLat(int y, String tile) {
        return 90 - Integer.parseInt(tile.split(",")[1]) / 4.0 - y * 0.25;
    }

    protected static double getLon(int x, String tile) {
        return -180 + Integer.parseInt(tile.split(",")[0]) / 4.0 + x * 0.25;
    }

    private Product getLcSubset(Product sourceProduct, Product lcProduct) {
        ReprojectionOp reprojectionOp = new ReprojectionOp();
        reprojectionOp.setSourceProduct("collocationProduct", sourceProduct);
        reprojectionOp.setSourceProduct(lcProduct);
        reprojectionOp.setParameterDefaultValues();
        return reprojectionOp.getTargetProduct();
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
        Product targetProduct = null;
        try {
            targetProduct = subsetOp.getTargetProduct();
        } catch (OperatorException exception) {
            if (exception.getMessage().contains("No intersection with source product boundary")) {
                return null;
            }
        }
        return targetProduct;
    }
}
