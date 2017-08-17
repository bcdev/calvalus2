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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    private final Product[] products;
    private final Product[] lcProducts;
    private final List<ZipFile> geoLookupTables;
    private final String targetCell; // "800,312"
    private final Configuration configuration;
    private final HashMap<String, AreaCalculator> areaCalculatorMap;
    private final SortedMap<String, Integer> bandToMinY;
    private final SortedMap<String, ProductData> data;

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, List<ZipFile> geoLookupTables, String targetCell, Configuration configuration) {
        this.products = products;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
        this.targetCell = targetCell;
        this.configuration = configuration;
        this.areaCalculatorMap = new HashMap<>();
        this.bandToMinY = new TreeMap<>();
        this.data = new TreeMap<>();
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
            CalvalusLogger.getLogger().info("        Handling product " + (i + 1) + "/" + products.length + "...");
            Product product = products[i];
            Product lcProduct = lcProducts[i];
            String tile = product.getName().split("_")[3].substring(0, 6);

            if (!geoLookupTable.containsKey(tile)) {
                continue;
            }

            String modisTile = product.getName().split("_")[3];
            File areaProductFile = new File(modisTile + ".hdf");
            CalvalusProductIO.copyFileToLocal(new Path("hdfs://calvalus/calvalus/projects/fire/aux/modis-areas-luts/areas-" + modisTile + ".nc"), areaProductFile, configuration);
            Product areaProduct = ProductIO.readProduct(areaProductFile);

            Band jd = product.getBand("classification");
            Band cl = product.getBand("uncertainty");
            Band numbObsFirstHalf = product.getBand("numObs1");
            Band numbObsSecondHalf = product.getBand("numObs2");
            Band lc = lcProduct.getBand("lccs_class");
            Band areas = areaProduct.getBand("areas");

            TreeSet<String> sourcePixelPoses = new TreeSet<>(geoLookupTable.get(tile));

//            ProductData jdData = ProductData.createInstance(jd.getDataType(), jd.getRasterWidth() * jd.getRasterHeight());
//            jd.readRasterData(0, 0, 4800, 4800, jdData);

//            ProductData clData = null;
//            if (cl != null) {
//                clData = ProductData.createInstance(cl.getDataType(), cl.getRasterWidth() * cl.getRasterHeight());
//                cl.readRasterData(0, 0, 4800, 4800, clData);
//            }
//
//            ProductData numbObsFirstHalfData = ProductData.createInstance(numbObsFirstHalf.getDataType(), numbObsFirstHalf.getRasterWidth() * numbObsFirstHalf.getRasterHeight());
//            numbObsFirstHalf.readRasterData(0, 0, 4800, 4800, numbObsFirstHalfData);
//
//            ProductData numbObsSecondHalfData = ProductData.createInstance(numbObsSecondHalf.getDataType(), numbObsSecondHalf.getRasterWidth() * numbObsSecondHalf.getRasterHeight());
//            numbObsSecondHalf.readRasterData(0, 0, 4800, 4800, numbObsSecondHalfData);
//
//            ProductData lcData = ProductData.createInstance(lc.getDataType(), lc.getRasterWidth() * lc.getRasterHeight());
//            lc.readRasterData(0, 0, 4800, 4800, lcData);

            ProductData areasData = ProductData.createInstance(data.areas);
            areas.readRasterData(0, 0, 4800, 4800, areasData);

//            System.arraycopy((double[])areasData.getElems(), 0, data.areas, 0, 4800 * 4800);
            int pixelIndex = 0;
            for (int x0 = 0; x0 < 4800; x0++) {
                for (int y0 = 0; y0 < 4800; y0++) {
                    byte sourceLC = (byte) getIntPixelValue(lc, pixelIndex);
//                    byte sourceLC = (byte) lcData.getElemIntAt(pixelIndex);
                    data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                    pixelIndex++;
                }
            }

            for (String sourcePixelPos : sourcePixelPoses) {
                String[] sppSplit = sourcePixelPos.split(",");
                int x0 = Integer.parseInt(sppSplit[0]);
                int y0 = Integer.parseInt(sppSplit[1]);
                pixelIndex = y0 * 4800 + x0;
//                int sourceJD = (int) jdData.getElemFloatAt(pixelIndex);
                int sourceJD = (int) getFloatPixelValue(jd, pixelIndex);
                int sourceStatusFirstHalf = getIntPixelValue(numbObsFirstHalf, pixelIndex);
//                int sourceStatusFirstHalf = numbObsFirstHalfData.getElemIntAt(pixelIndex);
                int sourceStatusSecondHalf = getIntPixelValue(numbObsSecondHalf, pixelIndex);
//                int sourceStatusSecondHalf = numbObsSecondHalfData.getElemIntAt(pixelIndex);
                data.statusPixelsFirstHalf[pixelIndex] = remap(sourceStatusFirstHalf);
                data.statusPixelsSecondHalf[pixelIndex] = remap(sourceStatusSecondHalf);
                if (isValidFirstHalfPixel(doyFirstOfMonth, doySecondHalf, sourceJD)) {
                    float sourceCL = getFloatPixelValue(cl, pixelIndex);
//                    float sourceCL = clData.getElemFloatAt(pixelIndex);
                    data.probabilityOfBurnFirstHalf[pixelIndex] = sourceCL;
                    data.burnedPixels[pixelIndex] = sourceJD;
                } else if (isValidSecondHalfPixel(doyLastOfMonth, doyFirstHalf, sourceJD)) {
                    float sourceCL = getFloatPixelValue(cl, pixelIndex);
//                    float sourceCL = clData.getElemFloatAt(pixelIndex);
                    data.probabilityOfBurnSecondHalf[pixelIndex] = sourceCL;
                    data.burnedPixels[pixelIndex] = sourceJD;
                }
            }
            CalvalusLogger.getLogger().info("...done.");
        }

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), false);

        return data;
    }

    float getFloatPixelValue(Band band, int pixelIndex) throws IOException {
        refreshCache(band, pixelIndex);
        int subPixelIndex = pixelIndex % 4800 + ((pixelIndex / 4800) % 480) * 4800;
        return data.get(band.getName()).getElemFloatAt(subPixelIndex);
    }

    private int getIntPixelValue(Band band, int pixelIndex) throws IOException {
        refreshCache(band, pixelIndex);
        int subPixelIndex = pixelIndex % 4800 + ((pixelIndex / 4800) % 480) * 4800;
        return data.get(band.getName()).getElemIntAt(subPixelIndex);
    }

    private void refreshCache(Band band, int pixelIndex) throws IOException {
        int currentMinY;
        String bandName = band.getName();
        if (bandToMinY.containsKey(bandName)) {
            currentMinY = bandToMinY.get(bandName);
        } else {
            int pixelIndexY = pixelIndex / 4800;
            currentMinY = pixelIndexY - pixelIndexY % 480;
        }

        int pixelIndexY = pixelIndex / 4800;
        if (pixelIndexY < currentMinY || pixelIndexY >= currentMinY + 480 || !data.containsKey(bandName)) {
            ProductData productData = ProductData.createInstance(band.getDataType(), band.getRasterWidth() * 480);
            band.readRasterData(0, pixelIndexY - pixelIndexY % 480, 4800, 480, productData);
            data.put(bandName, productData);
            currentMinY = pixelIndexY - pixelIndexY % 480;
            bandToMinY.put(bandName, currentMinY);
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
