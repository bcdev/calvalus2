package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.AreaCalculator;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.gpf.GPF;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    private final Product[] products;
    private final Product[] lcProducts;
    private final List<ZipFile> geoLookupTables;
    private final String targetCell; // "800,312"
    private final Configuration configuration;
    private final HashMap<String, AreaCalculator> areaCalculatorMap;
    private final Map<Band, Integer> bandToMinY;
    private final Map<Band, ProductData> data;

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, List<ZipFile> geoLookupTables, String targetCell, Configuration configuration) {
        this.products = products;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
        this.targetCell = targetCell;
        this.configuration = configuration;
        this.areaCalculatorMap = new HashMap<>();
        this.bandToMinY = new HashMap<>();
        this.data = new HashMap<>();
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        int targetCellX = x + Integer.parseInt(targetCell.split(",")[0]);
        int targetCellY = y + Integer.parseInt(targetCell.split(",")[1]);

        HashMap<String, Set<String>> geoLookupTable = getGeoLookupTable(targetCellX, targetCellY, geoLookupTables);

        SourceData data = new SourceData(4800, 4800);
        data.reset();

        if (geoLookupTable == null) {
            return data;
        }

        for (int i = 0; i < products.length; i++) {
            Product product = products[i];
            Product lcProduct = lcProducts[i];
            String tile = product.getName().split("_")[3].substring(0, 6);

            if (!geoLookupTable.containsKey(tile)) {
                continue;
            }

            AreaCalculator areaCalculator;
            String modisTile = product.getName().split("_")[3];
            if (areaCalculatorMap.containsKey(modisTile)) {
                areaCalculator = areaCalculatorMap.get(modisTile);
            } else {
                File refProductFile = new File(modisTile + ".hdf");
                CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/geolookup-refs/" + modisTile + ".hdf"), refProductFile, configuration);
                Product refProduct = ProductIO.readProduct(refProductFile);
                areaCalculator = new AreaCalculator(refProduct.getSceneGeoCoding());
                areaCalculatorMap.put(modisTile, areaCalculator);
            }

            Band jd = product.getBand("classification");
            Band cl = product.getBand("uncertainty");
            Band numbObsFirstHalf = product.getBand("numObs1");
            Band numbObsSecondHalf = product.getBand("numObs2");
            Band lc = lcProduct.getBand("lccs_class");

            Set<String> sourcePixelPoses = geoLookupTable.get(tile);

            int pixelIndex = 0;
            for (int x0 = 0; x0 < 4800; x0++) {
                for (int y0 = 0; y0 < 4800; y0++) {
                    if (sourcePixelPoses.contains(x0 + "," + y0)) {
                        int sourceJD = (int) getFloatPixelValue(jd, pixelIndex);
                        float sourceCL = getFloatPixelValue(cl, pixelIndex);
                        byte sourceLC = (byte) getIntPixelValue(lc, pixelIndex);
                        int sourceStatusFirstHalf = getIntPixelValue(numbObsFirstHalf, pixelIndex);
                        int sourceStatusSecondHalf = getIntPixelValue(numbObsSecondHalf, pixelIndex);
//                        int sourceJD = (int) jdData.getElemFloatAt(pixelIndex);
//                        float sourceCL = clData.getElemFloatAt(pixelIndex);
//                        byte sourceLC = (byte) lcData.getElemIntAt(pixelIndex);
//                        int sourceStatusFirstHalf = status1Data.getElemIntAt(pixelIndex);
//                        int sourceStatusSecondHalf = status1Data.getElemIntAt(pixelIndex);
                        data.statusPixelsFirstHalf[pixelIndex] = remap(sourceStatusFirstHalf);
                        data.statusPixelsSecondHalf[pixelIndex] = remap(sourceStatusSecondHalf);
                        data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                        data.lcClasses[pixelIndex] = (int) sourceLC;
                        if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, sourceJD)) {
                            data.probabilityOfBurnFirstHalf[pixelIndex] = sourceCL;
                            data.burnedPixels[pixelIndex] = sourceJD;
                        } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, sourceJD)) {
                            data.probabilityOfBurnSecondHalf[pixelIndex] = sourceCL;
                            data.burnedPixels[pixelIndex] = sourceJD;
                        }
                    }
                    if (data.areas[pixelIndex] == GridFormatUtils.NO_AREA) {
                        data.areas[pixelIndex] = areaCalculator.calculatePixelSize(x0, y0, product.getSceneRasterWidth() - 1, product.getSceneRasterHeight() - 1);
                    }
                    pixelIndex++;
                }
            }
        }

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), false);

        return data;
    }

    private float getFloatPixelValue(Band band, int pixelIndex) throws IOException {
        refreshCache(band, pixelIndex);
        return data.get(band).getElemFloatAt(pixelIndex % 480);
    }

    private int getIntPixelValue(Band band, int pixelIndex) throws IOException {
        refreshCache(band, pixelIndex);
        return data.get(band).getElemIntAt(pixelIndex % 480);
    }

    private void refreshCache(Band band, int pixelIndex) throws IOException {
        int currentMinY;
        if (bandToMinY.containsKey(band)) {
            currentMinY = bandToMinY.get(band);
        } else {
            currentMinY = pixelIndex - pixelIndex % 480;
        }

        if (pixelIndex < currentMinY || pixelIndex >= currentMinY + 480 || !data.containsKey(band)) {
            ProductData productData = ProductData.createInstance(band.getDataType(), band.getRasterWidth() * 480);
            band.readRasterData(0, pixelIndex - pixelIndex % 480, 4800, 480, productData);
            data.put(band, productData);
            currentMinY = pixelIndex - pixelIndex % 480;
            bandToMinY.put(band, currentMinY);
        }
    }

    private static HashMap<String, Set<String>> getGeoLookupTable(int targetCellX, int targetCellY, List<ZipFile> geoLookupTables) throws IOException {
        Gson gson = new Gson();
        HashMap<String, Set<String>> geoLookupTable = new HashMap<>();
        String lutName = String.format("modis-geo-lut-%s-%s.json", targetCellX, targetCellY);
        ZipEntry entry = null;
        ZipFile geoLookupTableFile = null;
        for (ZipFile geoLookupTable0 : geoLookupTables) {
            entry = geoLookupTable0.getEntry(lutName);
            geoLookupTableFile = geoLookupTable0;
            if (entry != null) {
                break;
            }
        }
        if (entry == null) {
            CalvalusLogger.getLogger().warning("No geolookup entry for target cell " + targetCellX + "/" + targetCellY);
            return null;
//            throw new IllegalStateException("No geolookup entry for target cell " + targetCellX + "/" + targetCellY);
        }
        try (InputStream lutStream = geoLookupTableFile.getInputStream(entry)) {
            geoLookupTable.putAll(gson.fromJson(new InputStreamReader(lutStream), GeoLutCreator.GeoLut.class));
        }
        return geoLookupTable;
    }

    private int remap(int status) {
        // 0 = Observed, 3=Not-observed and 4=Unburnable
        return status == 3 ? -1 : 1;
    }

    static boolean isValidFirstHalfPixel(int doyFirstOfMonth, int doySecondHalf, int pixel) {
        return pixel >= doyFirstOfMonth && pixel < doySecondHalf - 6;
    }

    static boolean isValidSecondHalfPixel(int doyLastOfMonth, int doyFirstHalf, int pixel) {
        return pixel > doyFirstHalf + 8 && pixel <= doyLastOfMonth;
    }

}
