package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.grid.AbstractFireGridDataSource;
import com.bc.calvalus.processing.fire.format.grid.GridFormatUtils;
import com.bc.calvalus.processing.fire.format.grid.SourceData;
import com.bc.ceres.core.ProgressMonitor;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;
import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.filter;

public class S2FireGridDataSource extends AbstractFireGridDataSource {

    private final String tile;
    private final Product[] sourceProducts;
    private final Product lcProduct;
    private final Logger log;
    public static final int STEP = 2;
    private List<ZipFile> geoLookupTables;
    private List<String> lcForTile = new ArrayList<>();

    public S2FireGridDataSource(String tile, Product sourceProducts[], Product lcProduct, List<ZipFile> geoLookupTables, Logger log) {
        this.tile = tile;
        this.sourceProducts = sourceProducts;
        this.lcProduct = lcProduct;
        this.log = log;
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

        int tileX = Integer.parseInt(tile.substring(4, 6));
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
            product.getBand("JD").readRasterDataFully(ProgressMonitor.NULL);
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

            int[] jdData = (int[]) product.getBand("JD").getData().getElems();
            String line;

            while ((line = br.readLine()) != null) {
                String[] splitLine = line.split(" ");
                int targetX = Integer.parseInt(splitLine[0]);
                int targetY = Integer.parseInt(splitLine[1]);
                int sourceX = Integer.parseInt(splitLine[2]);
                int sourceY = Integer.parseInt(splitLine[3]);
                int sourceJD = jdData[sourceY * product.getSceneRasterWidth() + sourceX];
                int oldValue = data.pixels[targetY * width + targetX];
                if (sourceJD > oldValue) {
                    data.pixels[targetY * width + targetX] = sourceJD;
                }
                if (!hasLcDoneForTile(utmTile)) {
                    gc.getGeoPos(new PixelPos(targetX, targetY), lcGeoPos);
                    lcProduct.getSceneGeoCoding().getPixelPos(lcGeoPos, lcPixelPos);
                    int sourceLC = lcData[(int) (lcPixelPos.y * lcProduct.getSceneRasterWidth() + lcPixelPos.x)];
                    data.lcClasses[targetY * width + targetX] = sourceLC;
                }
            }
            setLcDoneForTile(utmTile);
        }

        getAreas(gc, width, height, data.areas);

        data.patchCountFirstHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, width, height), true);
        data.patchCountSecondHalf = getPatchNumbers(GridFormatUtils.make2Dims(data.pixels, width, height), false);

        return data;
    }

    private boolean hasLcDoneForTile(String utmTile) {
        return lcForTile.contains(utmTile);
    }

    private boolean setLcDoneForTile(String utmTile) {
        return lcForTile.add(utmTile);
    }

    private static CrsGeoCoding getCrsGeoCoding(int width, int height, double upperLat, double leftLon) {
        try {
            return new CrsGeoCoding(DefaultGeographicCRS.WGS84, width, height, leftLon, upperLat, S2_GRID_PIXELSIZE, S2_GRID_PIXELSIZE);
        } catch (FactoryException | TransformException e) {
            throw new IllegalStateException("Unable to create temporary geo-coding", e);
        }
    }
}
