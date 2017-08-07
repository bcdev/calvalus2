package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private final String tile;
    private final Product[] sourceProducts;
    private final Product[] lcProducts;
    public static final int STEP = 2;
    private List<ZipFile> geoLookupTables;

    protected static final Logger LOG = CalvalusLogger.getLogger();

    public S2FireGridDataSource(String tile, Product sourceProducts[], Product[] lcProducts, List<ZipFile> geoLookupTables) {
        this.tile = tile;
        this.sourceProducts = sourceProducts;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        Product[] products = filter(tile, sourceProducts, x, y);
        if (products.length == 0) {
            return null;
        }

        SourceData data = new SourceData(1381, 1381);
        data.reset();

        for (int i = 0; i < products.length; i++) {
            Product product = products[i];
            Product lcProduct = lcProducts[i];
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
            AreaCalculator areaCalculator = new AreaCalculator(product.getSceneGeoCoding());
            Band jd = product.getBand("JD");
            Band cl = product.getBand("CL");
            Band lc = lcProduct.getBand("lccs_class");

            ProductData jdData = ProductData.createInstance(jd.getDataType(), jd.getRasterWidth() * jd.getRasterHeight());
            ProductData clData = ProductData.createInstance(cl.getDataType(), cl.getRasterWidth() * cl.getRasterHeight());
            ProductData lcData = ProductData.createInstance(lc.getDataType(), lc.getRasterWidth() * lc.getRasterHeight());

            jd.readRasterData(0, 0, jd.getRasterWidth(), jd.getRasterHeight(), jdData);
            cl.readRasterData(0, 0, jd.getRasterWidth(), jd.getRasterHeight(), clData);
            lc.readRasterData(0, 0, jd.getRasterWidth(), jd.getRasterHeight(), lcData);

            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split(" ");
                int targetX = Integer.parseInt(splitLine[0]);
                int targetY = Integer.parseInt(splitLine[1]);
                int sourceX = Integer.parseInt(splitLine[2]);
                int sourceY = Integer.parseInt(splitLine[3]);
                int targetPixelIndex = targetY * 1381 + targetX;

                int sourcePixelIndex = sourceY * product.getSceneRasterWidth() + sourceX;

                int sourceJD = (int) jdData.getElemFloatAt(sourcePixelIndex);
                float sourceCL = clData.getElemFloatAt(sourcePixelIndex);
                int oldValue = data.burnedPixels[targetPixelIndex];
                if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, sourceJD) || isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, sourceJD)) {
                    if (sourceJD > oldValue && sourceJD < 900) {
                        data.burnedPixels[targetPixelIndex] = sourceJD;
                        if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, sourceJD)) {
                            data.probabilityOfBurnFirstHalf[targetPixelIndex] = sourceCL;
                        } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, sourceJD)) {
                            data.probabilityOfBurnSecondHalf[targetPixelIndex] = sourceCL;
                        }
                    }

                    int sourceLC = lcData.getElemIntAt(sourcePixelIndex);
                    data.burnable[targetPixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    data.lcClasses[targetPixelIndex] = sourceLC;
                }
                if (sourceJD < 997 && sourceJD != -100) { // neither no-data, nor water, nor cloud -> observed pixel
                    // put observed pixel into first or second half of month
                    int productJD = getProductJD(product);
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, productJD)) {
                        data.statusPixelsFirstHalf[targetPixelIndex] = 1;
                    } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, productJD)) {
                        data.statusPixelsSecondHalf[targetPixelIndex] = 1;
                    }
                }
                if (data.areas[targetPixelIndex] == GridFormatUtils.NO_AREA) {
                    data.areas[targetPixelIndex] = areaCalculator.calculatePixelSize(sourceX, sourceY, product.getSceneRasterWidth() - 1, product.getSceneRasterHeight() - 1);
                }
            }
        }

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 1381, 1381), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 1381, 1381), false);

        return data;
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
