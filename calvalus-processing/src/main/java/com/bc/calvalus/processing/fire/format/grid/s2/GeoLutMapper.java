package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.commons.CalvalusLogger;
import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.datamodel.GeoPos;
import org.esa.snap.core.datamodel.PixelPos;
import org.esa.snap.core.datamodel.Product;

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

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.S2_GRID_PIXELSIZE;

public class GeoLutMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        CombineFileSplit inputSplit = (CombineFileSplit) context.getInputSplit();
        File localFile = CalvalusProductIO.copyFileToLocal(inputSplit.getPath(0), context.getConfiguration());
        String utmTile = getUtmTile(localFile);

        for (String tile : get2DegTiles(utmTile)) {
            int tileX = Integer.parseInt(tile.substring(4, 6));
            int tileY = Integer.parseInt(tile.substring(1, 3));
            CalvalusLogger.getLogger().info(String.format("Running for tile %s and utmTile %s", tile, utmTile));

            GeoPos geoPos = new GeoPos();
            PixelPos pixelPos = new PixelPos();
            Product p = ProductIO.readProduct(localFile);
            for (int y = 0; y < 8; y++) {
                double upperLat = 90 - tileY * 2 - y / 4.0;
                for (int x = 0; x < 8; x++) {
                    String pathname = "./" + tile + "-" + utmTile + "-" + x + "-" + y + ".dat";
                    File file = new File(pathname);
                    try (FileWriter fos = new FileWriter(file)) {
                        double currentLon = tileX * 2 - 180 + x / 4.0;
                        for (int x1 = 0; x1 < 1381; x1++) {
                            geoPos.lon = currentLon;
                            double currentLat = upperLat;
                            for (int y1 = 0; y1 < 1381; y1++) {
                                geoPos.lat = currentLat;
                                p.getSceneGeoCoding().getPixelPos(geoPos, pixelPos);
                                if (p.containsPixel(pixelPos)) {
                                    fos.write(x1 + " " + y1 + " " + (int) pixelPos.x + " " + (int) pixelPos.y + "\n");
                                }
                                currentLat -= S2_GRID_PIXELSIZE;
                            }
                            currentLon += S2_GRID_PIXELSIZE;
                        }
                    }
                    CalvalusLogger.getLogger().info(String.format("Done %02.2f%%.", (float) ((y * 8.0 + (x + 1)) * 100 / 64.0)));
                    context.progress();
                }
            }
            CalvalusLogger.getLogger().info("done, zipping results...");

            String zipFile = "./" + tile + "-" + utmTile + ".zip";
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
            Path targetPath = new Path("hdfs://calvalus/calvalus/projects/fire/aux/s2-geometry-lut/" + zipFile);
            FileUtil.copy(new File(zipFile), inputSplit.getPath(0).getFileSystem(context.getConfiguration()), targetPath, false, context.getConfiguration());
            Arrays.stream(new File(".").listFiles((dir, name) -> name.endsWith(".dat"))).forEach(File::delete);
            CalvalusLogger.getLogger().info("...done.");
        }
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
