package com.bc.calvalus.processing.fire.format.grid.s2;

import com.google.gson.Gson;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class S2GeoLutCreator {

    public static void main(String[] args) throws IOException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        GeoPos gp = new GeoPos();
        PixelPos pp = new PixelPos();

        int x = Integer.parseInt(args[0]);
        int y = Integer.parseInt(args[1]);

        String fileName = String.format("s2-geo-lut-%02d-%02d.json", x, y);

        GeoLut map = new GeoLut();

        for (int i = 2; i < args.length; i++) {
            String refProductFilename = args[i];
            String tile = refProductFilename.split("\\.")[0];

            Product refProduct = ProductIO.readProduct(refProductFilename);
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

    public static class GeoLut extends HashMap<String, Set<String>> {
        // only needed for GSON to serialise
    }

}
