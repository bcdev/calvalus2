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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class ModisFireGridDataSource extends AbstractFireGridDataSource {

    public static final double MODIS_AREA_SIZE = 53664.6683222854702276;
    private final Product[] products;
    private final Product[] lcProducts;
    private final String targetCell; // "800,312"

    public ModisFireGridDataSource(Product[] products, Product[] lcProducts, String targetCell) {
        super(4800, 4800);
        this.products = products;
        this.lcProducts = lcProducts;
        this.targetCell = targetCell;
    }

    @Override
    public SourceData readPixels(int x, int y) throws IOException {
        System.out.println(Instant.now().toString() + " Reading data for pixel " + x + "," + y + "...");
        int targetCellX = x + Integer.parseInt(targetCell.split(",")[0]);
        int targetCellY = y + Integer.parseInt(targetCell.split(",")[1]);


        HashMap<String, Set<String>> geoLookupTable = getGeoLookupTable(targetCellX, targetCellY);

        SourceData data = new SourceData(4800, 4800);
        data.reset();

        Arrays.fill(data.areas, MODIS_AREA_SIZE);

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

            float[] jdPixels = new float[4800 * 4800];
            jd.readPixels(0, 0, 4800, 4800, jdPixels);

            float[] clPixels = null;
            if (cl != null) {
                clPixels = new float[4800 * 4800];
                cl.readPixels(0, 0, 4800, 4800, clPixels);
            }

            int[] numObsPixels = new int[4800 * 4800];
            numbObs.readPixels(0, 0, 4800, 4800, numObsPixels);

            int[] lcPixels = new int[4800 * 4800];
            lc.readPixels(0, 0, 4800, 4800, lcPixels);

            for (String sourcePixelPos : geoLookupTable.get(tile)) {
                String[] sppSplit = sourcePixelPos.split(",");
                int x0 = Integer.parseInt(sppSplit[0]);
                int y0 = Integer.parseInt(sppSplit[1]);
                int pixelIndex = y0 * 4800 + x0;

                data.burnedPixels[pixelIndex] = jdPixels[pixelIndex];
                data.probabilityOfBurn[pixelIndex] = clPixels != null ? clPixels[pixelIndex] : 0.0F;
                int sourceLC = lcPixels[pixelIndex];
                data.lcClasses[pixelIndex] = sourceLC;
                int sourceStatus = numObsPixels[pixelIndex];
                data.statusPixels[pixelIndex] = remap(sourceStatus, data.statusPixels[pixelIndex]);
            }

        }

        data.patchCount = getPatchNumbers(GridFormatUtils.make2Dims(data.burnedPixels, 4800, 4800), GridFormatUtils.make2Dims(data.lcClasses, 4800, 4800), LcRemapping::isInBurnableLcClass);

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

    private static HashMap<String, Set<String>> getGeoLookupTable(int targetCellX, int targetCellY) throws IOException {
        Gson gson = new Gson();
        String lutName = String.format("modis-geo-lut-%s-%s.json", targetCellX < 10 ? "0" + targetCellX : targetCellX, targetCellY);
        Path path = Paths.get(lutName);
        if (!Files.exists(path)) {
            return null;
        }
        try (InputStream lutStream = Files.newInputStream(path)) {
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
