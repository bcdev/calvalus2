package com.bc.calvalus.processing.fire.format.grid.modis;

import com.google.gson.Gson;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class GeoLutCreator {

    private static final Map<String, Product> PRODUCTS = new HashMap<>();

    public static void main2(String[] args) throws IOException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        Product refProductSinusoidal = ProductIO.readProduct("C:\\ssd\\modis\\sinusoidal-ref.dim");
        GeoCoding geoCoding = refProductSinusoidal.getSceneGeoCoding();

        GeoPos ul = new GeoPos();
        GeoPos ur = new GeoPos();
        GeoPos lr = new GeoPos();
        GeoPos ll = new GeoPos();

        GeoPos gp = new GeoPos();
        PixelPos pp = new PixelPos();

        TileLut tileLut = new TileLut();

        int c = 0;
        for (int x = 0; x < 1440; x++) {
            for (int y = 0; y < 720; y++) {
                c++;
                if (c % 10000 == 0) {
                    System.out.println((c * 100.0) / (1440 * 720) + "%");
                }

                ul.setLocation(90 - y * 0.25, -180 + x * 0.25);
                ur.setLocation(90 - y * 0.25, -180 + (x + 1) * 0.25);
                lr.setLocation(90 - (y + 1) * 0.25, -180 + (x + 1) * 0.25);
                ll.setLocation(90 - y * 0.25, -180 + (x + 1) * 0.25);

                Set<String> inputTiles = new HashSet<>();
                geoCoding.getPixelPos(ul, pp);
                String tile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (exists(tile)) {
                    inputTiles.add(tile);
                }
                geoCoding.getPixelPos(ur, pp);
                tile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (exists(tile)) {
                    inputTiles.add(tile);
                }
                geoCoding.getPixelPos(lr, pp);
                tile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (exists(tile)) {
                    inputTiles.add(tile);
                }
                geoCoding.getPixelPos(ll, pp);
                tile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (exists(tile)) {
                    inputTiles.add(tile);
                }

                if (!inputTiles.isEmpty()) {
                    tileLut.put("" + x + "," + y, inputTiles);
                }
            }
        }
        Gson gson = new Gson();
        try (FileWriter fw = new FileWriter(String.format("c:\\ssd\\modis-geo-luts\\modis-tiles-lut.txt"))) {
            gson.toJson(tileLut, fw);
        }
    }

    public static class TileLut extends HashMap<String, Set<String>> {
        // only needed for GSON to serialise
    }

    public static class GeoLut extends HashMap<String, Set<String>> {
        // only needed for GSON to serialise
    }

    static boolean exists(String tile) throws IOException {
        Optional<Path> pathOptional = Files.list(Paths.get("D:\\workspace\\fire-cci\\modis-for-lc")).filter(path -> path.toString().contains(tile)).findFirst();
        return pathOptional.isPresent();
    }

    public static void main(String[] args) throws IOException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        Product refProductSinusoidal = ProductIO.readProduct("C:\\ssd\\modis\\sinusoidal-ref.dim");
        GeoCoding geoCoding = refProductSinusoidal.getSceneGeoCoding();

        GeoPos ul = new GeoPos();
        GeoPos ur = new GeoPos();
        GeoPos lr = new GeoPos();
        GeoPos ll = new GeoPos();

        GeoPos gp = new GeoPos();
        PixelPos pp = new PixelPos();

        Set<String> missingRefProducts = new HashSet<>();
        int c = 0;
        for (int x = 0; x < 1440; x++) {
            for (int y = 0; y < 720; y++) {
                c++;
                String fileName = String.format("c:\\ssd\\modis-geo-luts\\modis-geo-lut-%02d-%02d.json", x, y);
                if (c % 10000 == 0) {
                    System.out.println(String.format("%.3f%%", (c * 100.0) / (1440 * 720)));
                }

                if (Files.exists(Paths.get(fileName))) {
                    continue;
                }

                GeoLut map = new GeoLut();
                ul.setLocation(90 - y * 0.25, -180 + x * 0.25);
                ur.setLocation(90 - y * 0.25, -180 + (x + 1) * 0.25);
                ll.setLocation(90 - (y + 1) * 0.25, -180 + x * 0.25);
                lr.setLocation(90 - (y + 1) * 0.25, -180 + (x + 1) * 0.25);

                Set<String> inputTiles = new HashSet<>();
                geoCoding.getPixelPos(ul, pp);
                inputTiles.add(String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40));
                geoCoding.getPixelPos(ur, pp);
                inputTiles.add(String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40));
                geoCoding.getPixelPos(lr, pp);
                inputTiles.add(String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40));
                geoCoding.getPixelPos(ll, pp);
                inputTiles.add(String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40));

                for (String tile : inputTiles) {
                    Product refProduct = getRefProduct(tile);
                    if (refProduct == null) {
                        missingRefProducts.add(tile);
                        continue;
                    }
                    GeoCoding refGeoCoding = refProduct.getSceneGeoCoding();

                    double lon = -180 + x * 0.25;
                    double lat = 90 - y * 0.25;

                    List<String> pixelPoses = new ArrayList<>();

                    gp.lat = lat;
                    gp.lon = lon;

                    for (pp.x = 0; pp.x < refProduct.getSceneRasterWidth(); pp.x++) {
                        for (pp.y = 0; pp.y < refProduct.getSceneRasterHeight(); pp.y++) {
                            refGeoCoding.getGeoPos(pp, gp);
                            boolean isInLatRange = gp.lat <= 90 - y * 0.25 && gp.lat >= 90 - (y + 1) * 0.25;
                            boolean isInLonRange = gp.lon >= -180 + x * 0.25 && gp.lon <= -180 + (x + 1) * 0.25;

                            if (isInLatRange && isInLonRange) {
                                pixelPoses.add(String.format("%s,%s", (int) pp.x, (int) pp.y));
                            }
                        }
                    }

                    map.put(tile, new HashSet<>(pixelPoses));
                }
                boolean empty = true;
                for (Set<String> strings : map.values()) {
                    empty &= strings.isEmpty();
                }
                if (!empty) {
                    Gson gson = new Gson();
                    try (FileWriter fw = new FileWriter(fileName)) {
                        gson.toJson(map, fw);
                    }
                }
            }
        }
        System.out.println("The following tiles have no ref product:");
        System.out.println(missingRefProducts);
    }

    private static Product getRefProduct(String tile) throws IOException {
        if (PRODUCTS.containsKey(tile)) {
            return PRODUCTS.get(tile);
        }
        Optional<Path> pathOptional = Files.list(Paths.get("D:\\workspace\\fire-cci\\modis-for-lc")).filter(path -> path.toString().contains(tile)).findFirst();
        if (!pathOptional.isPresent()) {
            return null;
        }
        Path first = pathOptional.get();
        Product product = ProductIO.readProduct(first.toFile());
        PRODUCTS.put(tile, product);
        return product;
    }

}
