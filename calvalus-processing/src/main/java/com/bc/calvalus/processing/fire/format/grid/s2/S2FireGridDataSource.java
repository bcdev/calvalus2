package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.esa.snap.core.gpf.common.MergeOp;
import org.esa.snap.core.gpf.common.reproject.ReprojectionOp;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.getMatchingProductIndices;

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

        List<Integer> productIndices = getMatchingProductIndices(tile, sourceProducts, x, y);
        if (productIndices.size() == 0) {
            CalvalusLogger.getLogger().warning("No input product available for pixel x=" + x + ", y=" + y);
            return null;
        }

        SourceData data = new SourceData(DIMENSION, DIMENSION);
        data.reset();

        // tile == x210y94
        int lon0 = -180 + Integer.parseInt(tile.split("y")[0].replace("x", ""));
        int lat0 = -90 + Integer.parseInt(tile.split("y")[1].replace("y", ""));
        double pixelSize = 0.25 / 5490.0;

        Product mergeProduct = new Product("temp", "temp", DIMENSION, DIMENSION);
        try {
            mergeProduct.setSceneGeoCoding(new CrsGeoCoding(
                    DefaultGeographicCRS.WGS84, DIMENSION, DIMENSION, lon0, lat0, pixelSize, pixelSize
            ));
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException(e);
        }

        List<Product> reprojectedSourceProducts = new ArrayList<>();

        int productIndex = 0;
        for (Integer i : productIndices) {
            ReprojectionOp reprojectionOp = new ReprojectionOp();
            reprojectionOp.setParameterDefaultValues();
            reprojectionOp.setSourceProduct("collocationProduct", mergeProduct);
            reprojectionOp.setSourceProduct(sourceProducts[i]);
            Product targetProduct = reprojectionOp.getTargetProduct();
            reprojectedSourceProducts.add(targetProduct);
            targetProduct.getBand("JD").setName("JD_" + productIndex);
            targetProduct.getBand("CL").setName("CL_" + productIndex);
            productIndex++;
        }

        MergeOp mergeOp = new MergeOp();
        mergeOp.setParameterDefaultValues();
        mergeOp.setSourceProduct("masterProduct", reprojectedSourceProducts.get(0));
        Product[] productsToMerge = new Product[reprojectedSourceProducts.size() - 1];
        System.arraycopy(reprojectedSourceProducts.toArray(new Product[0]), 1, productsToMerge, 0, reprojectedSourceProducts.size() - 1);
        mergeOp.setSourceProducts(productsToMerge);
        Product mergedProduct = mergeOp.getTargetProduct();
        StringBuilder jdExpression = new StringBuilder();
        for (int i = 0; i < productIndex; i++) {
            jdExpression.append("(JD_" + i + " > 0) ? JD_" + i + " : ");
        }
        jdExpression.append(" 0");
        StringBuilder clExpression = new StringBuilder();
        for (int i = 0; i < productIndex; i++) {
            clExpression.append("(CL_" + i + " > 0) ? CL_" + i + " : ");
        }
        clExpression.append(" 0");
        mergedProduct.addBand("merged_JD", jdExpression.toString());
        mergedProduct.addBand("merged_CL", clExpression.toString());

        for (int pixelIndex = 0; pixelIndex < DIMENSION * DIMENSION; pixelIndex++) {
            Band jd = mergedProduct.getBand("merged_JD");
            Band cl = mergedProduct.getBand("merged_CL");
//            Band lc = lcProduct.getBand("lccs_class");

            String key = mergedProduct.getName();

            int sourceJD = (int) getFloatPixelValue(jd, key, pixelIndex);
            boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
            if (isValidPixel) {
                // set burned pixel value consistently with CL value -- both if burned pixel is valid
                data.burnedPixels[pixelIndex] = sourceJD;

                float sourceCL;
                if (cl != null) {
                    sourceCL = getFloatPixelValue(cl, key, pixelIndex);
                } else {
                    sourceCL = 0.0F;
                }

                sourceCL = scale(sourceCL);
                data.probabilityOfBurn[pixelIndex] = sourceCL;
            }

//            int sourceLC = getIntPixelValue(lc, key, pixelIndex);
//            data.burnable[pixelIndex] = LcRemappingS2.isInBurnableLcClass(sourceLC);
//            data.lcClasses[pixelIndex] = sourceLC;
            if (sourceJD < 997 && sourceJD != -100) { // neither no-data, nor water, nor cloud -> observed pixel
                int productJD = getProductJD(sourceProducts[0]);
                if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, productJD)) {
                    data.statusPixels[pixelIndex] = 1;
                }
            }

            if (data.areas[pixelIndex] == GridFormatUtils.NO_AREA) {
                AreaCalculator areaCalculator = new AreaCalculator(mergedProduct.getSceneGeoCoding());
                data.areas[pixelIndex] = areaCalculator.calculatePixelSize(pixelIndex % DIMENSION, pixelIndex / DIMENSION, DIMENSION - 1, DIMENSION - 1);
            }

        }

/*
        PixelPos sourcePixelPos = new PixelPos();
        GeoPos targetGeoPos = new GeoPos();

        for (int targetPixelIndex = 0; targetPixelIndex < DIMENSION * DIMENSION; targetPixelIndex++) {
            if (targetPixelIndex % DIMENSION == 0) {
                System.out.println(targetPixelIndex);
            }

            targetGeoPos.lat = lat0 + (float) targetPixelIndex / (float) DIMENSION * pixelSize;
            targetGeoPos.lon = lon0 + (float) targetPixelIndex % (float) DIMENSION * pixelSize;

            for (Integer i : productIndices) {
                Product product = sourceProducts[i];
                Product lcProduct = lcProducts[i];

                AreaCalculator areaCalculator = new AreaCalculator(product.getSceneGeoCoding());
                Band jd = product.getBand("JD");
                Band cl = product.getBand("CL");
                Band lc = lcProduct.getBand("lccs_class");

                product.getSceneGeoCoding().getPixelPos(targetGeoPos, sourcePixelPos);
                if (!sourcePixelPos.isValid()) {
                    continue;
                }
                int sourcePixelIndex = (int) sourcePixelPos.y * DIMENSION + (int) sourcePixelPos.x;

                String key = product.getName();

                int sourceJD = (int) getFloatPixelValue(jd, key, sourcePixelIndex);
                boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                if (isValidPixel) {
                    // set burned pixel value consistently with CL value -- both if burned pixel is valid
                    data.burnedPixels[targetPixelIndex] = sourceJD;

                    float sourceCL;
                    if (cl != null) {
                        sourceCL = getFloatPixelValue(cl, key, sourcePixelIndex);
                    } else {
                        sourceCL = 0.0F;
                    }

                    sourceCL = scale(sourceCL);
                    data.probabilityOfBurn[sourcePixelIndex] = sourceCL;
                }

                int sourceLC = getIntPixelValue(lc, key, sourcePixelIndex);
                data.burnable[sourcePixelIndex] = LcRemappingS2.isInBurnableLcClass(sourceLC);
                data.lcClasses[sourcePixelIndex] = sourceLC;
                if (sourceJD < 997 && sourceJD != -100) { // neither no-data, nor water, nor cloud -> observed pixel
                    int productJD = getProductJD(product);
                    if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, productJD)) {
                        data.statusPixels[sourcePixelIndex] = 1;
                    }
                }

                if (data.areas[sourcePixelIndex] == GridFormatUtils.NO_AREA) {
                    data.areas[sourcePixelIndex] = areaCalculator.calculatePixelSize((int) sourcePixelPos.x, (int) sourcePixelPos.y, product.getSceneRasterWidth() - 1, product.getSceneRasterHeight() - 1);
                }

            }
        }
*/


/*

        for (Integer i : productIndices) {
            Product product = sourceProducts[i];
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
            BufferedReader br = new BufferedReader(new InputStreamReader(geoLookupTable.getInputStream(currentEntry), StandardCharsets.UTF_8));

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

                int sourcePixelIndex = y0 * DIMENSION + x0;

                int sourceJD = (int) getFloatPixelValue(jd, key, sourcePixelIndex);
                boolean isValidPixel = isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD);
                if (isValidPixel) {
                    // set burned pixel value consistently with CL value -- both if burned pixel is valid
                    data.burnedPixels[sourcePixelIndex] = sourceJD;

                    float sourceCL;
                    if (cl != null) {
                        sourceCL = getFloatPixelValue(cl, key, sourcePixelIndex);
                    } else {
                        sourceCL = 0.0F;
                    }

                    sourceCL = scale(sourceCL);
                    data.probabilityOfBurn[sourcePixelIndex] = sourceCL;
                }

                int sourceLC = getIntPixelValue(lc, key, sourcePixelIndex);
                data.burnable[sourcePixelIndex] = LcRemappingS2.isInBurnableLcClass(sourceLC);
                data.lcClasses[sourcePixelIndex] = sourceLC;
                if (sourceJD < 997 && sourceJD != -100) { // neither no-data, nor water, nor cloud -> observed pixel
                    int productJD = getProductJD(product);
                    if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, productJD)) {
                        data.statusPixels[sourcePixelIndex] = 1;
                    }
                }

                if (data.areas[sourcePixelIndex] == GridFormatUtils.NO_AREA) {
                    data.areas[sourcePixelIndex] = areaCalculator.calculatePixelSize(x0, y0, product.getSceneRasterWidth() - 1, product.getSceneRasterHeight() - 1);
                }
            }
        }

*/

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, DIMENSION, DIMENSION), GridFormatUtils.make2Dims(data.burnable, DIMENSION, DIMENSION));

        return data;
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
