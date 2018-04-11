package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private static final int DIMENSION = 5490;
    private final String tile;
    private final Product[] sourceProducts;
    private final Product[] lcProducts;
    private List<ZipFile> geoLookupTables;

    protected static final Logger LOG = CalvalusLogger.getLogger();

    public S2FireGridDataSource(String tile, Product sourceProducts[], Product[] lcProducts, List<ZipFile> geoLookupTables) {
        super(549, DIMENSION);
        this.tile = tile;
        this.sourceProducts = sourceProducts;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        CalvalusLogger.getLogger().warning("Reading data for pixel x=" + x + ", y=" + y);
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        CalvalusLogger.getLogger().info("All products:");
        CalvalusLogger.getLogger().info(Arrays.toString(Arrays.stream(sourceProducts).map(Product::getName).toArray()));

        Product[] products = filter(tile, sourceProducts, x, y);
//        Product[] products = sourceProducts;
        if (products.length == 0) {
            CalvalusLogger.getLogger().warning("No input product available for pixel x=" + x + ", y=" + y);
            return null;
        }

        CalvalusLogger.getLogger().info("Products after filtering:");
        CalvalusLogger.getLogger().info(Arrays.toString(Arrays.stream(products).map(Product::getName).toArray()));

        SourceData data = new SourceData(DIMENSION, DIMENSION);
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
                continue;
//                throw new IllegalStateException("No geo-lookup table for ");
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

            String key = product.getName();

            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split(" ");
                int x0 = Integer.parseInt(splitLine[2]);
                int y0 = Integer.parseInt(splitLine[3]);

                int pixelIndex = y0 * DIMENSION + x0;

                int sourceJD = (int) getFloatPixelValue(jd, key, pixelIndex);
                boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                if (isValidPixel) {
                    data.burnedPixels[pixelIndex] = sourceJD;
                    float sourceCL;
                    if (cl != null) {
                        sourceCL = getFloatPixelValue(cl, key, pixelIndex);
                    } else {
                        sourceCL = 0.0F;
                    }
                    data.probabilityOfBurn[pixelIndex] = sourceCL;
                }

                int sourceLC = getIntPixelValue(lc, key, pixelIndex);
                data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                data.lcClasses[pixelIndex] = sourceLC;
                if (sourceJD < 997 && sourceJD != -100) { // neither no-data, nor water, nor cloud -> observed pixel
                    int productJD = getProductJD(product);
                    if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, productJD)) {
                        data.statusPixels[pixelIndex] = 1;
                    }
                }

                if (data.areas[pixelIndex] == GridFormatUtils.NO_AREA) {
                    data.areas[pixelIndex] = areaCalculator.calculatePixelSize(x0, y0, product.getSceneRasterWidth() - 1, product.getSceneRasterHeight() - 1);
                }
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, DIMENSION, DIMENSION));

        return data;
    }

    static int getProductJD(Product product) {
        String productDate = product.getName().substring(product.getName().lastIndexOf("-") + 1);// BA-T31NBJ-20160219T101925
        return LocalDate.parse(productDate, DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss")).get(ChronoField.DAY_OF_YEAR);
    }

}
