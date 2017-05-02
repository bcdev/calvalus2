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

    private static final Map<String, GeoCoding> tiles = new HashMap<>();

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

        Result result = new Result();

        int c = 0;
        for (int x = 0; x < 1440; x++) {
            for (int y = 0; y < 720; y++) {
                c++;
                if (c % 10000 == 0) {
                    System.out.println((c * 100.0) / (1440 * 720) + "%");
                }

                HashMap<String, Set<String>> map = new HashMap<>();
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
                    result.put("" + x + "," + y, inputTiles);
                }
            }
        }
        Gson gson = new Gson();
        try (FileWriter fw = new FileWriter(String.format("c:\\ssd\\modis-geo-luts\\modis-tiles-lut.txt"))) {
            gson.toJson(result, fw);
        }
    }

    public static class Result extends HashMap<String, Set<String>> {

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


        int c = 0;
        for (int x = 0; x < 1440; x++) {
            for (int y = 0; y < 720; y++) {
                c++;
                String fileName = String.format("c:\\ssd\\modis-geo-luts\\modis-geo-lut-%02d-%02d.json", x, y);
                if (Files.exists(Paths.get(fileName))) {
                    continue;
                }
                if (c % 10000 == 0) {
                    System.out.println(String.format("%.3f%%", (c * 100.0) / (1440 * 720)));
                }

                HashMap<String, Set<String>> map = new HashMap<>();
                ul.setLocation(90 - y * 0.25, -180 + x * 0.25);
                ur.setLocation(90 - y * 0.25, -180 + (x + 1) * 0.25);
                lr.setLocation(90 - (y + 1) * 0.25, -180 + (x + 1) * 0.25);
                ll.setLocation(90 - y * 0.25, -180 + (x + 1) * 0.25);

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
                    GeoCoding refGeoCoding = getRefGeoCoding(tile);

                    if (refGeoCoding == null) {
                        continue;
                    }

                    double lon = -180 + x * 0.25;
                    double lat = 90 - y * 0.25;

                    gp.setLocation(lat, lon);
                    refGeoCoding.getPixelPos(gp, pp);
                    pp.x = (int) pp.x;
                    pp.y = (int) pp.y;
                    refGeoCoding.getGeoPos(pp, gp);
                    double refLat = gp.lat;
                    double refLon = gp.lon;

                    pp.x = pp.x + 1;
                    double pixelSizeX;
                    double pixelSizeY;
                    if (refProductSinusoidal.containsPixel(pp)) {
                        refGeoCoding.getGeoPos(pp, gp);
                        pixelSizeX = Math.abs(gp.lon - refLon);
                        pp.x = pp.x - 1;
                        pp.y = pp.y + 1;
                        if (!refProductSinusoidal.containsPixel(pp)) {
                            pp.y = pp.y - 2;
                        }
                        refGeoCoding.getGeoPos(pp, gp);
                        pixelSizeY = Math.abs(gp.lat - refLat);
                    } else {
                        pp.x = pp.x - 2;
                        refGeoCoding.getGeoPos(pp, gp);
                        pixelSizeX = Math.abs(gp.lon - refLon);

                        pp.x = pp.x + 2;
                        pp.y = pp.y + 1;
                        if (!refProductSinusoidal.containsPixel(pp)) {
                            pp.y = pp.y - 2;
                        }
                        refGeoCoding.getGeoPos(pp, gp);
                        pixelSizeY = Math.abs(gp.lat - refLat);
                    }

                    List<String> pixelPoses = new ArrayList<>();

                    for (double currentLon = lon; currentLon < lon + 0.25; currentLon += pixelSizeX) {
                        for (double currentLat = lat; currentLat > lat - 0.25; currentLat -= pixelSizeY) {
                            gp.setLocation(currentLat, currentLon);
                            refGeoCoding.getPixelPos(gp, pp);
                            if (refProductSinusoidal.containsPixel(pp)) {
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
    }

    private static GeoCoding getRefGeoCoding(String tile) throws IOException {
        if (tiles.containsKey(tile)) {
            return tiles.get(tile);
        }
        Optional<Path> pathOptional = Files.list(Paths.get("D:\\workspace\\fire-cci\\modis-for-lc")).filter(path -> path.toString().contains(tile)).findFirst();
        if (!pathOptional.isPresent()) {
            return null;
        }
        Path first = pathOptional.get();
        GeoCoding geoCoding = ProductIO.readProduct(first.toFile()).getSceneGeoCoding();
        tiles.put(tile, geoCoding);
        return geoCoding;
    }

}
