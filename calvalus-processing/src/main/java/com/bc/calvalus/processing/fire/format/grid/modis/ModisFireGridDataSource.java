package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.google.gson.Gson;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    private final Product[] products;
    private final Product[] lcProducts;
    private final ZipFile geoLookupTables;
    private final String targetTile; // "801,312"

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, ZipFile geoLookupTables, String targetTile) {
        this.products = products;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
        this.targetTile = targetTile;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        int width = 4800;
        int height = 4800;

        x += Integer.parseInt(targetTile.split(",")[0]);
        y += Integer.parseInt(targetTile.split(",")[1]);

        SourceData data = new SourceData(width, height);
        data.reset();

        CrsGeoCoding gc = getCrsGeoCoding(width, height, getUpperLat(y), getLeftLon(x));

        String lutName = String.format("modis-geo-lut-%s-%s.json", x, y);
        ZipEntry entry = geoLookupTables.getEntry(lutName);
        Gson gson = new Gson();
        HashMap<String, Set<String>> geoLookupTable;
        try (InputStream lutStream = geoLookupTables.getInputStream(entry)) {
            geoLookupTable = gson.fromJson(new InputStreamReader(lutStream), GeoLutCreator.GeoLut.class);
        }

        for (int i = 0; i < products.length; i++) {
            Product product = products[i];
            Product lcProduct = lcProducts[i];
            if (geoLookupTable == null) {
                throw new IllegalStateException("No geo-lookup table for pixel '" + x + " / '" + y);
            }

            String tile = product.getName().split("_")[3].substring(0, 6);

            if (!geoLookupTable.containsKey(tile)) {
                throw new IllegalStateException("Non-existing entry for tile '" + tile + "' and target pixel " + x + " " + y);
            }

            Band jd = product.getBand("classification");
            Band cl = product.getBand("uncertainty");
            Band numbObsFirstHalf = product.getBand("numObs1");
            Band numbObsSecondHalf = product.getBand("numObs2");
            Band lc = lcProduct.getBand("lccs_class");
            ProductData jdData = ProductData.createInstance(jd.getDataType(), 1);
            ProductData clData = ProductData.createInstance(cl.getDataType(), 1);
            ProductData status1Data = ProductData.createInstance(numbObsFirstHalf.getDataType(), 1);
            ProductData status2Data = ProductData.createInstance(numbObsSecondHalf.getDataType(), 1);
            ProductData lcData = ProductData.createInstance(lc.getDataType(), 1);

            Set<String> sourcePixelPoses = geoLookupTable.get(tile);

            for (String sourcePixelPos : sourcePixelPoses) {
                int sourceX = Integer.parseInt(sourcePixelPos.split(",")[0]);
                int sourceY = Integer.parseInt(sourcePixelPos.split(",")[1]);
                int pixelIndex = sourceY * width + sourceX;
                jd.readRasterData(sourceX, sourceY, 1, 1, jdData);
                cl.readRasterData(sourceX, sourceY, 1, 1, clData);
                numbObsFirstHalf.readRasterData(sourceX, sourceY, 1, 1, status1Data);
                numbObsSecondHalf.readRasterData(sourceX, sourceY, 1, 1, status2Data);
                int sourceJD = (int) jdData.getElemFloatAt(0);
                float sourceCL = clData.getElemFloatAt(0);
                byte sourceLC = (byte) lcData.getElemIntAt(0);
                int sourceStatusFirstHalf = status1Data.getElemIntAt(0);
                int sourceStatusSecondHalf = status1Data.getElemIntAt(0);
                data.statusPixelsFirstHalf[pixelIndex] = sourceStatusFirstHalf;
                data.statusPixelsSecondHalf[pixelIndex] = sourceStatusSecondHalf;
                if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, sourceJD)) {
                    data.probabilityOfBurnFirstHalf[pixelIndex] = sourceCL;
                    data.burnedPixels[pixelIndex] = sourceJD;
                    data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    data.lcClasses[pixelIndex] = (int) sourceLC;
                } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, sourceJD)) {
                    data.probabilityOfBurnSecondHalf[pixelIndex] = sourceCL;
                    data.burnedPixels[pixelIndex] = sourceJD;
                    data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    data.lcClasses[pixelIndex] = (int) sourceLC;
                }
            }
        }

        getAreas(gc, width, height, data.areas);

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, width, height), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, width, height), false);

        return data;
}

    private static CrsGeoCoding getCrsGeoCoding(int width, int height, double upperLat, double leftLon) {
        try {
            return new CrsGeoCoding(DefaultGeographicCRS.WGS84, width, height, leftLon, upperLat, 10.0/4800.0, 10.0/4800.0);
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Unable to create temporary geo-coding", e);
        }
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, int pixel) {
        return pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, int pixel) {
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth;
    }

    static double getUpperLat(int y) {
        if (y < 0 || y > 719) {
            throw new IllegalArgumentException("invalid value of y: " + y + "; y has to be between 0 and 719.");
        }
        return 90 - 0.25 * y;
    }

    static double getLeftLon(int x) {
        if (x < 0 || x > 1439) {
            throw new IllegalArgumentException("invalid value of x: " + x + "; x has to be between 0 and 1439.");
        }
        return -180 + 0.25 * x;
    }
}
