package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private final String tile;
    private final Product[] sourceProducts;
    private final Product lcProduct;
    public static final int STEP = 2;
    private List<ZipFile> geoLookupTables;

    public S2FireGridDataSource(String tile, Product sourceProducts[], Product lcProduct, List<ZipFile> geoLookupTables) {
        this.tile = tile;
        this.sourceProducts = sourceProducts;
        this.lcProduct = lcProduct;
        this.geoLookupTables = geoLookupTables;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product[] products = filter(tile, sourceProducts, x, y);
        if (products.length == 0) {
            return null;
        }

        int width = 1381;
        int height = 1381;

        int tileX = Integer.parseInt(tile.substring(4));
        int tileY = Integer.parseInt(tile.substring(1, 3));
        double upperLat = 90 - tileY * STEP - STEP + (y + 1) / 4.0;
        double leftLon = tileX * STEP - 180 + x / 4.0;

        SourceData data = new SourceData(width, height);
        data.reset();

        GeoPos lcGeoPos = new GeoPos();
        PixelPos lcPixelPos = new PixelPos();
        lcProduct.getBand("lcclass").readRasterDataFully(ProgressMonitor.NULL);
        byte[] lcData = (byte[]) lcProduct.getBand("lcclass").getData().getElems();

        CrsGeoCoding gc = getCrsGeoCoding(width, height, upperLat, leftLon);
        for (Product product : products) {
            ZipFile geoLookupTable = null;
            for (ZipFile lookupTable : geoLookupTables) {
                if (lookupTable.getName().matches(".*" + product.getName().substring(4, 9) + ".*")) {
                    geoLookupTable = lookupTable;
                    break;
                }
            }
            if (geoLookupTable == null) {
                throw new IllegalStateException("No geo-lookup table for ");
            }

            ZipEntry currentEntry = null;
            Enumeration<? extends ZipEntry> entries = geoLookupTable.entries();
            String utmTile = product.getName().substring(4, 9);
            while (entries.hasMoreElements()) {
                ZipEntry zipEntry = entries.nextElement();
                if (zipEntry.getName().matches(tile + "-" + utmTile + "-" + x + "-" + y + "\\.dat")) {
                    currentEntry = zipEntry;
                    break;
                }
            }
            if (currentEntry == null) {
                throw new IllegalStateException("Zip entry '" + tile + "-" + utmTile + "-" + x + "-" + y + ".dat' not found in " + geoLookupTable.getName() + "; check the auxiliary data.");
            }
            BufferedReader br = new BufferedReader(new InputStreamReader(geoLookupTable.getInputStream(currentEntry), "UTF-8"));

            String line;
            Band jd = product.getBand("JD");
            Band cl = product.getBand("CL");
            ProductData jdData = ProductData.createInstance(jd.getDataType(), 1);
            ProductData clData = ProductData.createInstance(cl.getDataType(), 1);
            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split(" ");
                int targetX = Integer.parseInt(splitLine[0]);
                int targetY = Integer.parseInt(splitLine[1]);
                int sourceX = Integer.parseInt(splitLine[2]);
                int sourceY = Integer.parseInt(splitLine[3]);
                jd.readRasterData(sourceX, sourceY, 1, 1, jdData);
                cl.readRasterData(sourceX, sourceY, 1, 1, clData);
                int sourceJD = (int) jdData.getElemFloatAt(0);
                float sourceCL = clData.getElemFloatAt(0);
                int pixelIndex = targetY * width + targetX;
                int oldValue = data.pixels[pixelIndex];
                if (sourceJD > oldValue && sourceJD < 900) {
                    data.pixels[pixelIndex] = sourceJD;
                }
                if (sourceJD < 999) {
                    int productJD = getProductJD(product);
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, productJD)) {
                        data.probabilityOfBurnFirstHalf[pixelIndex] = sourceCL;
                        data.statusPixelsFirstHalf[pixelIndex] = 1;
                    } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, productJD)) {
                        data.probabilityOfBurnSecondHalf[pixelIndex] = sourceCL;
                        data.statusPixelsSecondHalf[pixelIndex] = 1;
                    }
                }
            }
        }

        for (int y1 = 0; y1 < height; y1++) {
            for (int x1 = 0; x1 < width; x1++) {
                int pixelIndex = y1 * width + x1;
                gc.getGeoPos(new PixelPos(x1, y1), lcGeoPos);
                lcProduct.getSceneGeoCoding().getPixelPos(lcGeoPos, lcPixelPos);
                if (lcProduct.containsPixel(lcPixelPos)) {
                    int sourceLC = lcData[(int) lcPixelPos.y * lcProduct.getSceneRasterWidth() + (int) lcPixelPos.x];
                    data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    int jdValue = data.pixels[pixelIndex];
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, jdValue)
                            || isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, jdValue)) {
                        data.lcClasses[pixelIndex] = sourceLC;
                    }
                }
            }
        }

        getAreas(gc, width, height, data.areas);

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, width, height), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, width, height), false);

        return data;
    }

    private static CrsGeoCoding getCrsGeoCoding(int width, int height, double upperLat, double leftLon) {
        try {
            return new CrsGeoCoding(DefaultGeographicCRS.WGS84, width, height, leftLon, upperLat, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE);
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Unable to create temporary geo-coding", e);
        }
    }

    static int getProductJD(Product product) {
        String productDate = product.getName().substring(product.getName().lastIndexOf("-") + 1);// BA-T31NBJ-20160219T101925
        return LocalDate.parse(productDate, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")).get(ChronoField.DAY_OF_YEAR);
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, int pixel) {
        return pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, int pixel) {
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth;
    }

}
