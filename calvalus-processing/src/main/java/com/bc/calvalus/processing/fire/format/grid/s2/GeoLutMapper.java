package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.gpf.common.SubsetOp;

import java.awt.*;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class GeoLutMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        File localFile = CalvalusProductIO.copyFileToLocal(inputSplit.getPath(0), context.getConfiguration());
        String utmTile = getUtmTile(localFile);

        for (String tile : get2DegTiles(utmTile)) {
            String zipFile = "./" + tile + "-" + utmTile + ".zip";
            Path targetPath = new Path("hdfs://calvalus/calvalus/projects/fire/aux/s2-geolookup/" + zipFile);

            if (FileSystem.get(context.getConfiguration()).exists(targetPath)) {
                CalvalusLogger.getLogger().info(String.format("LUT for tile %s and utmTile %s already exists.", tile, utmTile));
                continue;
            }

            boolean hasFoundPixel = extract(localFile, utmTile, tile);
            if (!hasFoundPixel) {
                CalvalusLogger.getLogger().warning("No pixels found!");
            }
            CalvalusLogger.getLogger().info("done, zipping results...");

            FileOutputStream fout = new FileOutputStream(zipFile);
            try (ZipOutputStream zipOutput = new ZipOutputStream(new BufferedOutputStream(fout));) {
                File[] datFiles = new File(".").listFiles((dir, name) -> name.endsWith(".dat"));
                for (File datFile : datFiles) {
                    byte[] buffer = new byte[1024];
                    FileInputStream fin = new FileInputStream(datFile);
                    zipOutput.putNextEntry(new ZipEntry(datFile.getName()));
                    int length;
                    while ((length = fin.read(buffer)) > 0) {
                        zipOutput.write(buffer, 0, length);
                    }
                    zipOutput.closeEntry();
                    fin.close();
                }
            }

            CalvalusLogger.getLogger().info("...done, copying to final directory and cleaning...");
            FileUtil.copy(new File(zipFile), inputSplit.getPath(0).getFileSystem(context.getConfiguration()), targetPath, false, context.getConfiguration());
            Arrays.stream(new File(".").listFiles((dir, name) -> name.endsWith(".dat"))).forEach(File::delete);
            CalvalusLogger.getLogger().info("...done.");
        }
    }

    static boolean extract(File localFile, String utmTile, String tile) throws IOException {
        CalvalusLogger.getLogger().info(String.format("Running for tile %s and utmTile %s", tile, utmTile));

        GeoPos gp = new GeoPos();
        PixelPos pp = new PixelPos();
        Product refProduct = ProductIO.readProduct(localFile);
        GeoCoding refGeoCoding = refProduct.getSceneGeoCoding();
        boolean hasFoundPixel = false;
        int x0 = Integer.parseInt(tile.split("y")[0].substring(1));
        int y0 = Integer.parseInt(tile.split("y")[1]);

        double topLat;
        double leftLon;
        double bottomLat;
        double rightLon;

        for (int y = 0; y < 8; y++) {
            for (int x = 0; x < 8; x++) {

                GeometryFactory factory = new GeometryFactory();
                WKTReader wktReader = new WKTReader(factory);
                topLat = getTopLat(y0, y);
                leftLon = getLeftLon(x0, x);
                bottomLat = getBottomLat(y0, y);
                rightLon = getRightLon(x0, x);
                Geometry geometry;
                try {
                    String wellKnownText = String.format("POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))", leftLon, topLat, rightLon, topLat, rightLon, bottomLat, leftLon, bottomLat, leftLon, topLat);
                    CalvalusLogger.getLogger().info(wellKnownText);
                    geometry = wktReader.read(wellKnownText);
                } catch (ParseException e) {
                    throw new IllegalStateException("Must not come here");
                }

                String pathname = "./" + tile + "-" + utmTile + "-" + x + "-" + y + ".dat";
                File file = new File(pathname);
                try (FileWriter fos = new FileWriter(file)) {
                    Rectangle rectangle = SubsetOp.computePixelRegion(refProduct, geometry, 0);
                    System.out.println(rectangle);
                    for (pp.y = rectangle.y; pp.y < rectangle.y + rectangle.height; pp.y++) {
                        for (pp.x = rectangle.x; pp.x < rectangle.x + rectangle.width; pp.x++) {
                            refGeoCoding.getGeoPos(pp, gp);
                            boolean isInLatRange = gp.lat >= getBottomLat(y0, y) && gp.lat <= getTopLat(y0, y);
                            boolean isInLonRange = gp.lon >= getLeftLon(x0, x) && gp.lon <= getRightLon(x0, x);
                            if (isInLatRange && isInLonRange) {
                                fos.write(x + " " + y + " " + (int) pp.x + " " + (int) pp.y + "\n");
                                hasFoundPixel = true;
                            }
                        }
                    }
                }
                CalvalusLogger.getLogger().info(String.format("%02.2f%%.", (float) ((y * 8 + x) * 100.0 / 64.0)));
            }
        }

        return hasFoundPixel;
    }

    static double getBottomLat(int y0, int y) {
        return getTopLat(y0, y) - 0.25;
    }

    static double getTopLat(int y0, int y) {
        return -90 + y0 - (y * 0.25);
    }

    static double getLeftLon(int x0, int x) {
        return x0 - 180 + x * 0.25;
    }

    static double getRightLon(int x0, int x) {
        return getLeftLon(x0, x) + 0.25;
    }

    private static String getUtmTile(File localFile) {
        String filename = localFile.getName(); // S2A_OPER_PRD_MSIL2A_PDMC_20160103T141745_R064_V20160103T085753_20160103T085753_T35QPE.tif
        int startIndex = filename.lastIndexOf("_") + 2;
        return filename.substring(startIndex, startIndex + 5);
    }

    private static String[] get2DegTiles(String utmTile) {
        List<String> result = new ArrayList<>();
        Properties tiles = new Properties();
        try {
            tiles.load(GeoLutMapper.class.getResourceAsStream("areas-tiles-2deg.properties"));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        for (Map.Entry<Object, Object> entry : tiles.entrySet()) {
            if (entry.getValue().toString().contains(utmTile)) {
                result.add(entry.getKey().toString());
            }
        }
        return result.toArray(new String[0]);
    }
}
