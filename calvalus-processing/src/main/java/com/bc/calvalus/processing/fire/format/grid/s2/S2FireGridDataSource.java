package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private final String tile;
    private final Product[] sourceProducts;
    private final Product[] lcProducts;
    public static final int STEP = 2;
    private List<ZipFile> geoLookupTables;

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

        List<SourceData> allSourceData = new ArrayList<>();
        List<int[]> indices = new ArrayList<>();

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
            Band jd = product.getBand("JD");
            Band cl = product.getBand("CL");
            Band lc = lcProduct.getBand("lccs_class");
            ProductData jdData = ProductData.createInstance(jd.getDataType(), 1);
            ProductData clData = ProductData.createInstance(cl.getDataType(), 1);
            ProductData lcData = ProductData.createInstance(cl.getDataType(), 1);
            PixelPos pixelPos = new PixelPos();
            while ((line = br.readLine()) != null) {
                SourceData data = new SourceData(1, 1);
                String[] splitLine = line.split(" ");
                int sourceX = Integer.parseInt(splitLine[2]);
                int sourceY = Integer.parseInt(splitLine[3]);
                jd.readRasterData(sourceX, sourceY, 1, 1, jdData);
                cl.readRasterData(sourceX, sourceY, 1, 1, clData);
                lc.readRasterData(sourceX, sourceY, 1, 1, lcData);
                int sourceJD = (int) jdData.getElemFloatAt(0);
                float sourceCL = clData.getElemFloatAt(0);
                int oldValue = data.burnedPixels[0];
                if (sourceJD > oldValue && sourceJD < 900) {
                    data.burnedPixels[0] = sourceJD;
                }
                if (sourceJD < 998) { // neither no-data nor cloud -> observed pixel
                    // put observed pixel into first or second half of month
                    int productJD = getProductJD(product);
                    if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, productJD)) {
                        data.probabilityOfBurnFirstHalf[0] = sourceCL;
                        data.statusPixelsFirstHalf[0] = 1;
                    } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, productJD)) {
                        data.probabilityOfBurnSecondHalf[0] = sourceCL;
                        data.statusPixelsSecondHalf[0] = 1;
                    }
                }
                pixelPos.x = sourceX;
                pixelPos.y = sourceY;
                int sourceLC = lcData.getElemIntAt(0);
                data.burnable[0] = LcRemapping.isInBurnableLcClass(sourceLC);
                int jdValue = data.burnedPixels[0];
                if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, jdValue)
                        || isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, jdValue)) {
                    data.lcClasses[0] = sourceLC;
                }
                allSourceData.add(data);
            }
        }

        SourceData data = SourceData.merge(allSourceData);

        int width = (int) Math.sqrt(data.width);
        getAreas(indices, data.areas);

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, width, width), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, width, width), false);

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
