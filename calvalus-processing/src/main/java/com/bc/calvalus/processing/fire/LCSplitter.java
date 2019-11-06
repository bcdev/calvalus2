package com.bc.calvalus.processing.fire;

import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.GPF;

import java.awt.Rectangle;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Splits LC map into tiles relevant for Fire-CCI processing.
 */
public class LCSplitter {

    private static final int RASTER_LENGTH = 3600;

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.err.println("LCSplitter <year> <tilesfile> <mapfile> <destdir>");
            System.err.println("LCSplitter 2000 'D:\\workspace\\fire-cci\\lc-data-to-split\\tiles.txt' 'C:\\temp\\formatting_development\\ESACCI-LC-L4-LCCS-Map-300m-P5Y-2000-v1.6.1.nc' 'D:\\workspace\\fire-cci\\splitted-lc-data'");
            System.exit(1);
        }
        String year = args[0];
        String tilesPath = args[1];
        String mapPath = args[2];
        String outputPath = args[3];

        FileSystem fileSystem = FileSystems.getDefault();
        List<String> tiles = readTiles(tilesPath);

        Path inputPath = fileSystem.getPath(mapPath);
        Product product = ProductIO.readProduct(inputPath.toFile());

        for (String tile : tiles) {
            System.out.format("creating tile %s\n", tile);
            int h = new Integer(tile.substring(1, 3));
            int v = new Integer(tile.substring(4, 6));
            int x0 = h * 3600;
            int y0 = v * 3600;
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("bandNames", new String[]{"lccs_class"});
            parameters.put("region", new Rectangle(x0, y0, 3600, 3600));
            Product subset = GPF.createProduct("Subset", parameters, product);
            ProductIO.writeProduct(subset, outputPath + fileSystem.getSeparator() + "lc-" + year + "-" + tile + ".nc", "NetCDF4-CF");
        }
    }

    private static List<String> readTiles(String tilesPath) throws IOException {
        List<String> tiles = new ArrayList<>();
        Path path = FileSystems.getDefault().getPath(tilesPath);
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                tiles.add(line);
            }
        }
        return tiles;
    }

}
