package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemappingS2;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.SubsetOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.logging.Logger;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

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

        double lon0 = -180 + Integer.parseInt(tile.split("y")[0].replace("x", "")) + x * 0.25;
        double lat0 = -90 + Integer.parseInt(tile.split("y")[1].replace("y", "")) + 1 - (y + 1) * 0.25;

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
            Product lcSubset = getLcSubset(jdSubset, lcProduct);

            Band jd = jdSubset.getBand("JD");
            Band cl = clSubset.getBand("CL");
            Band lc = lcSubset.getBand("band_1");

            AreaCalculator areaCalculator = new AreaCalculator(jdSubset.getSceneGeoCoding());

            PixelPos pixelPos = new PixelPos();
            GeoPos geoPos = new GeoPos();
            for (int lineIndex = 0; lineIndex < jdSubset.getSceneRasterHeight(); lineIndex++) {
                pixelPos.x = 0;
                pixelPos.y = lineIndex;
                lc.getGeoCoding().getGeoPos(pixelPos, geoPos);
                int width = jdSubset.getSceneRasterWidth();

                int[] jdPixels = new int[width];
                float[] clPixels = new float[width];
                int[] lcPixels = new int[width];

                jd.readPixels(0, lineIndex, width, 1, jdPixels);
                cl.readPixels(0, lineIndex, width, 1, clPixels);
                lc.readPixels(0, lineIndex, width, 1, lcPixels);

                if (geoPos.lat < -34.84) {
                    Arrays.fill(lcPixels, 10);
                }

                for (int x0 = 0; x0 < width; x0++) {
                    int sourceJD = jdPixels[x0];
                    float sourceCL = clPixels[x0];
                    int sourceLC = lcPixels[x0];
                    data.burnable[targetPixelIndex] = LcRemappingS2.isInBurnableLcClass(sourceLC);
                    boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                    if (isValidPixel) {
                        // set burned pixel value consistently with CL value -- both if burned pixel is valid
                        data.burnedPixels[targetPixelIndex] = sourceJD;
                    }
                    data.probabilityOfBurn[targetPixelIndex] = sourceCL;

                    data.lcClasses[targetPixelIndex] = sourceLC;
                    if (sourceJD >= 0) { // neither no-data, nor water, nor cloud -> observed pixel
                        data.statusPixels[targetPixelIndex] = 1;
                    } else {
                        data.statusPixels[targetPixelIndex] = 0;
                    }

                    data.areas[targetPixelIndex] = areaCalculator.calculatePixelSize(x0, lineIndex, width - 1, jdSubset.getSceneRasterHeight() - 1);
                    targetPixelIndex++;
                }
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, totalWidth, totalHeight), GridFormatUtils.make2Dims(data.burnable, totalWidth, totalHeight));
        return data;
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
        return subsetOp.getTargetProduct();
    }

    static int getProductJD(Product product) {
        String productDate = product.getName().substring(product.getName().lastIndexOf("-") + 1);// BA-T31NBJ-20160219T101925
        return LocalDate.parse(productDate, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")).get(ChronoField.DAY_OF_YEAR);
    }

}
