package com.bc.calvalus.processing.fire.format.grid.modis;

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

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, List<ZipFile> geoLookupTables, String targetCell, Configuration configuration) {
        this.products = products;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
        this.targetCell = targetCell;
        this.configuration = configuration;
        this.areaCalculatorMap = new HashMap<>();
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis();

        int targetCellX = x + Integer.parseInt(targetCell.split(",")[0]);
        int targetCellY = y + Integer.parseInt(targetCell.split(",")[1]);

        HashMap<String, Set<String>> geoLookupTable = getGeoLookupTable(targetCellX, targetCellY, geoLookupTables);

        SourceData data = new SourceData(4800, 4800);

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

            ProductData jdData = ProductData.createInstance(jd.getDataType(), jd.getRasterWidth() * jd.getRasterHeight());
            ProductData clData = ProductData.createInstance(cl.getDataType(), cl.getRasterWidth() * cl.getRasterHeight());
            ProductData status1Data = ProductData.createInstance(numbObsFirstHalf.getDataType(), numbObsFirstHalf.getRasterWidth() * numbObsFirstHalf.getRasterHeight());
            ProductData status2Data = ProductData.createInstance(numbObsSecondHalf.getDataType(), numbObsSecondHalf.getRasterWidth() * numbObsSecondHalf.getRasterHeight());
            ProductData lcData = ProductData.createInstance(lc.getDataType(), lc.getRasterWidth() * lc.getRasterHeight());

            jd.readRasterData(0, 0, jd.getRasterWidth(), jd.getRasterHeight(), jdData);
            cl.readRasterData(0, 0, cl.getRasterWidth(), cl.getRasterHeight(), clData);
            numbObsFirstHalf.readRasterData(0, 0, numbObsFirstHalf.getRasterWidth(), numbObsFirstHalf.getRasterHeight(), status1Data);
            numbObsSecondHalf.readRasterData(0, 0, numbObsSecondHalf.getRasterWidth(), numbObsSecondHalf.getRasterHeight(), status2Data);
            lc.readRasterData(0, 0, lc.getRasterWidth(), lc.getRasterHeight(), lcData);

            Set<String> sourcePixelPoses = geoLookupTable.get(tile);

            int pixelIndex = 0;
            for (int x0 = 0; x0 < 4800; x0++) {
                for (int y0 = 0; y0 < 4800; y0++) {
                    if (sourcePixelPoses.contains(x0 + "," + y0)) {
                        int sourceJD = (int) jdData.getElemFloatAt(pixelIndex);
                        float sourceCL = clData.getElemFloatAt(pixelIndex);
                        byte sourceLC = (byte) lcData.getElemIntAt(pixelIndex);
                        int sourceStatusFirstHalf = status1Data.getElemIntAt(pixelIndex);
                        int sourceStatusSecondHalf = status1Data.getElemIntAt(pixelIndex);
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
            throw new IllegalStateException("No geolookup entry for target cell " + targetCellX + "/" + targetCellY);
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
