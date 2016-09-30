package com.bc.calvalus.processing.fire.format.pixel;

import com.bc.calvalus.processing.beam.CalvalusProductIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public class S2GeometryLUT {

    private Map<String, Integer[]> lut;
    private final Configuration conf;


    public S2GeometryLUT(Configuration conf) {
        this.conf = conf;
    }

    public int getRasterWidth(String tile) {
        lazyInit();
        return lut.get(tile)[0];
    }

    public int getRasterHeight(String tile) {
        lazyInit();
        return lut.get(tile)[1];
    }

    public int getUpperLeftLon(String tile) {
        lazyInit();
        return lut.get(tile)[2];
    }

    public int getUpperLeftLat(String tile) {
        lazyInit();
        return lut.get(tile)[3];
    }

    public int getLowerRightLon(String tile) {
        lazyInit();
        return lut.get(tile)[4];
    }

    public int getLowerRightLat(String tile) {
        lazyInit();
        return lut.get(tile)[5];
    }

    private void lazyInit() {
        if (lut == null) {
            Path path = new Path("hdfs://calvalus/calvalus/projects/fire/aux/s2-geometry-lut", "s2-geometry-lut.txt");
            try {
                CalvalusProductIO.copyFileToLocal(path, conf);
                try (Stream<String> stream = Files.lines(Paths.get("s2-geometry-lut.txt"))) {
                    stream.forEach(s -> {
                        String[] split = s.split(",");
                        lut.put(split[0], new Integer[]{Integer.parseInt(split[1]), Integer.parseInt(split[2])});
                    });
                }
            } catch (IOException e) {
                throw new IllegalStateException("Could not copy file '" + path + "' to local.", e);
            }
        }
    }

}
