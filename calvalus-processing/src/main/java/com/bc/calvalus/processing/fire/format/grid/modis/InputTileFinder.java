package com.bc.calvalus.processing.fire.format.grid.modis;

import com.google.gson.Gson;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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

        TileLut tileLut = new TileLut();

        Set<String> inputTiles = new HashSet<>();

        int x = Integer.parseInt(args[0]);
        int y = Integer.parseInt(args[1]);

        for (int y0 = 0; y0 < ModisGridMapper.WINDOW_SIZE; y0++) {
            for (int x0 = 0; x0 < ModisGridMapper.WINDOW_SIZE; x0++) {
                ul.setLocation(90 - (y + y0) * 0.25, -180 + (x + x0) * 0.25);
                ur.setLocation(90 - (y + y0) * 0.25, -180 + (x + x0 + 1) * 0.25);
                ll.setLocation(90 - (y + y0 + 1) * 0.25, -180 + (x + x0) * 0.25);
                lr.setLocation(90 - (y + y0 + 1) * 0.25, -180 + (x + x0 + 1) * 0.25);

                geoCoding.getPixelPos(ul, pp);
                String inputTile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (Arrays.asList(allValidInputTiles).contains(inputTile)) {
                    inputTiles.add(inputTile);
                }
                geoCoding.getPixelPos(ur, pp);
                inputTile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (Arrays.asList(allValidInputTiles).contains(inputTile)) {
                    inputTiles.add(inputTile);
                }
                geoCoding.getPixelPos(lr, pp);
                inputTile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (Arrays.asList(allValidInputTiles).contains(inputTile)) {
                    inputTiles.add(inputTile);
                }
                geoCoding.getPixelPos(ll, pp);
                inputTile = String.format("h%1$02dv%2$02d", (int) pp.x / 40, (int) pp.y / 40);
                if (Arrays.asList(allValidInputTiles).contains(inputTile)) {
                    inputTiles.add(inputTile);
                }
            }
        }
        tileLut.put("" + x + "," + y, inputTiles);

        Gson gson = new Gson();
        try (FileWriter fw = new FileWriter("inputTiles.txt")) {
            gson.toJson(tileLut, fw);
        }
    }

    public static class TileLut extends HashMap<String, Set<String>> {
        // only needed for GSON to serialise
    }

    private static String[] allValidInputTiles = {
            "h03v06",
            "h03v07",
            "h07v05",
            "h07v06",
            "h08v03",
            "h08v04",
            "h08v05",
            "h08v06",
            "h08v07",
            "h08v08",
            "h08v09",
            "h09v03",
            "h09v04",
            "h09v05",
            "h09v06",
            "h09v07",
            "h09v08",
            "h09v09",
            "h10v02",
            "h10v03",
            "h10v04",
            "h10v05",
            "h10v06",
            "h10v07",
            "h10v08",
            "h10v09",
            "h10v10",
            "h11v01",
            "h11v02",
            "h11v03",
            "h11v04",
            "h11v05",
            "h11v06",
            "h11v07",
            "h11v08",
            "h11v09",
            "h11v10",
            "h11v11",
            "h11v12",
            "h12v01",
            "h12v02",
            "h12v03",
            "h12v04",
            "h12v05",
            "h12v07",
            "h12v08",
            "h12v09",
            "h12v10",
            "h12v11",
            "h12v12",
            "h12v13",
            "h13v01",
            "h13v02",
            "h13v03",
            "h13v04",
            "h13v08",
            "h13v09",
            "h13v10",
            "h13v11",
            "h13v12",
            "h13v13",
            "h13v14",
            "h14v01",
            "h14v02",
            "h14v03",
            "h14v04",
            "h14v09",
            "h14v10",
            "h14v11",
            "h14v14",
            "h15v00",
            "h15v01",
            "h15v02",
            "h15v07",
            "h16v00",
            "h16v01",
            "h16v02",
            "h16v05",
            "h16v06",
            "h16v07",
            "h16v08",
            "h17v00",
            "h17v01",
            "h17v02",
            "h17v03",
            "h17v04",
            "h17v05",
            "h17v06",
            "h17v07",
            "h17v08",
            "h18v00",
            "h18v01",
            "h18v02",
            "h18v03",
            "h18v04",
            "h18v05",
            "h18v06",
            "h18v07",
            "h18v08",
            "h18v09",
            "h19v00",
            "h19v01",
            "h19v02",
            "h19v03",
            "h19v04",
            "h19v05",
            "h19v06",
            "h19v07",
            "h19v08",
            "h19v09",
            "h19v10",
            "h19v11",
            "h19v12",
            "h20v00",
            "h20v01",
            "h20v02",
            "h20v03",
            "h20v04",
            "h20v05",
            "h20v06",
            "h20v07",
            "h20v08",
            "h20v09",
            "h20v10",
            "h20v11",
            "h20v12",
            "h21v00",
            "h21v01",
            "h21v02",
            "h21v03",
            "h21v04",
            "h21v05",
            "h21v06",
            "h21v07",
            "h21v08",
            "h21v09",
            "h21v10",
            "h21v11",
            "h22v01",
            "h22v02",
            "h22v03",
            "h22v04",
            "h22v05",
            "h22v06",
            "h22v07",
            "h22v08",
            "h22v09",
            "h22v10",
            "h22v11",
            "h22v13",
            "h22v14",
            "h23v01",
            "h23v02",
            "h23v03",
            "h23v04",
            "h23v05",
            "h23v06",
            "h23v07",
            "h23v08",
            "h23v09",
            "h23v10",
            "h23v11",
            "h24v02",
            "h24v03",
            "h24v04",
            "h24v05",
            "h24v06",
            "h24v07",
            "h25v02",
            "h25v03",
            "h25v04",
            "h25v05",
            "h25v06",
            "h25v07",
            "h25v08",
            "h26v02",
            "h26v03",
            "h26v04",
            "h26v05",
            "h26v06",
            "h26v07",
            "h26v08",
            "h27v03",
            "h27v04",
            "h27v05",
            "h27v06",
            "h27v07",
            "h27v08",
            "h27v09",
            "h27v11",
            "h27v12",
            "h28v04",
            "h28v05",
            "h28v06",
            "h28v07",
            "h28v08",
            "h28v09",
            "h28v11",
            "h28v12",
            "h28v13",
            "h29v05",
            "h29v06",
            "h29v07",
            "h29v08",
            "h29v09",
            "h29v10",
            "h29v11",
            "h29v12",
            "h29v13",
            "h30v07",
            "h30v08",
            "h30v09",
            "h30v10",
            "h30v11",
            "h30v12",
            "h30v13",
            "h31v07",
            "h31v09",
            "h31v10",
            "h31v11",
            "h31v12",
            "h31v13",
            "h32v07",
            "h32v09",
            "h32v10",
            "h32v12",
            "h33v09",
            "h33v10",
            "h33v11",
            "h34v10",
            "h35v10"
    };
}
