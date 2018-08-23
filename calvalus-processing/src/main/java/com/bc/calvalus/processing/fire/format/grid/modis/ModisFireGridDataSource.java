package com.bc.calvalus.processing.fire.format.grid.modis;

import com.bc.calvalus.processing.fire.format.LcRemapping;
import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.google.gson.Gson;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    public static final double MODIS_AREA_SIZE = 53664.6683222854702276;
    private final Product[] products;
    private final Product[] lcProducts;
    private final List<ZipFile> geoLookupTables;
    private final String targetCell; // "800,312"
    private final SortedSet<Long> alreadyVisitedPixelPoses;

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, List<ZipFile> geoLookupTables, String targetCell) {
        super(480, 4800);
        this.products = products;
        this.lcProducts = lcProducts;
        this.geoLookupTables = geoLookupTables;
        this.targetCell = targetCell;
        this.alreadyVisitedPixelPoses = new TreeSet<>();
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
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

            Band jd = product.getBand("classification");
            Band cl = product.getBand("uncertainty");
            Band numbObs = product.getBand("numObs1");
            Band lc = lcProduct.getBand("lccs_class");

            TreeSet<String> sourcePixelPoses = new TreeSet<>((o1, o2) -> {
                int y1 = Integer.parseInt(o1.split(",")[1]);
                int y2 = Integer.parseInt(o2.split(",")[1]);
                if (y1 == y) {
                    return 0;
                } else if (y1 < y2) {
                    return -1;
                } else return 1;
            });
            sourcePixelPoses.addAll(geoLookupTable.get(tile));

            for (String sourcePixelPos : sourcePixelPoses) {
                String[] sppSplit = sourcePixelPos.split(",");
                int x0 = Integer.parseInt(sppSplit[0]);
                int y0 = Integer.parseInt(sppSplit[1]);
                long key = createKey(tile, x0, y0);
                if (alreadyVisitedPixelPoses.contains(key)) {
                    continue;
                }
                alreadyVisitedPixelPoses.add(key);
                int pixelIndex = y0 * 4800 + x0;
                int sourceJD = (int) getFloatPixelValue(jd, tile, pixelIndex);
                if (isValidPixel(doyFirstOfMonth, doyLastOfMonth, sourceJD)) {
                    data.burnedPixels[pixelIndex] = sourceJD;
                }

                float sourceCL;
                if (cl != null) {
                    sourceCL = getFloatPixelValue(cl, tile, pixelIndex) / 100.0F;
                } else {
                    sourceCL = 0.0F;
                }
                data.probabilityOfBurn[pixelIndex] = sourceCL;

                int sourceLC = getIntPixelValue(lc, tile, pixelIndex);
                data.burnable[pixelIndex] = LcRemapping.isInBurnableLcClass(sourceLC);
                data.lcClasses[pixelIndex] = sourceLC;
                int sourceStatus = getIntPixelValue(numbObs, tile, pixelIndex);
                data.statusPixels[pixelIndex] = remap(sourceStatus, data.statusPixels[pixelIndex]);

                data.areas[pixelIndex] = MODIS_AREA_SIZE;
            }
        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800));

        return data;
    }

    static long createKey(String tile, int x0, int y0) {
        String tileX = tile.substring(1, 3);
        String tileY = tile.substring(4, 6);
        String yPart;
        if (y0 < 10) {
            yPart = "000" + y0;
        } else if (y0 < 100) {
            yPart = "00" + y0;
        } else if (y0 < 1000) {
            yPart = "0" + y0;
        } else {
            yPart = "" + y0;
        }
        String xPart;
        if (x0 < 10) {
            xPart = "000" + x0;
        } else if (x0 < 100) {
            xPart = "00" + x0;
        } else if (x0 < 1000) {
            xPart = "0" + x0;
        } else {
            xPart = "" + x0;
        }
        return Long.parseLong(tileX + tileY + xPart + yPart);
    }

    private static HashMap<String, Set<String>> getGeoLookupTable(int targetCellX, int targetCellY, List<ZipFile> geoLookupTables) throws IOException {
        Gson gson = new Gson();
        String lutName = String.format("modis-geo-lut-%s-%s.json", targetCellX < 10 ? "0" + targetCellX : targetCellX, targetCellY);
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
            return null;
        }
        try (InputStream lutStream = geoLookupTableFile.getInputStream(entry)) {
            return gson.fromJson(new InputStreamReader(lutStream), GeoLutCreator.GeoLut.class);
        }
    }

    private int remap(int status, int oldStatus) {
        // 0 = Observed, 3=Not-observed and 4=Unburnable
        boolean nothingSetBefore = oldStatus == 0;
        if (nothingSetBefore) {
            return status == 3 ? -1 : 1;
        } else {
            return oldStatus == -1 ? (status == 3 ? -1 : 1) : 1;
        }
    }

}
