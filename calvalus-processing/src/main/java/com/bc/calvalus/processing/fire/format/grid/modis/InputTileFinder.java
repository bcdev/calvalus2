package com.bc.calvalus.processing.fire.format.grid.modis;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InputTileFinder {

    public static void main(String[] args) throws IOException {

        System.setProperty("snap.dataio.netcdf.metadataElementLimit", "0");

        Product refProductSinusoidal = ProductIO.readProduct("sinusoidal-ref.dim");
        GeoCoding geoCoding = refProductSinusoidal.getSceneGeoCoding();

        GeoPos ul = new GeoPos();
        GeoPos ur = new GeoPos();
        GeoPos lr = new GeoPos();
        GeoPos ll = new GeoPos();

        PixelPos pp = new PixelPos();

        int x = Integer.parseInt(args[0]);
        int y = Integer.parseInt(args[1]);

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

        try (FileWriter fw = new FileWriter("inputTiles.txt")) {
            for (String inputTile : inputTiles) {
                fw.write(inputTile + " ");
            }
        }
    }

}
