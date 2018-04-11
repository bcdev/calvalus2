package com.bc.calvalus.processing.fire;

import com.bc.calvalus.processing.fire.format.grid.modis.ModisGridInputFormat;
import com.google.gson.Gson;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class FireBAInputFormatTest {

    @Test
    public void testStringMatchesTile() throws Exception {

        assertTrue(FireBAInputFormat.stringMatchesTile(19, 0, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v00h19.nc"));
        assertTrue(FireBAInputFormat.stringMatchesTile(19, 18, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v18h19.nc"));
        assertTrue(FireBAInputFormat.stringMatchesTile(3, 2, "CCI-Fire-MERIS-SDR-L3-300m-v1.0-2008-04-02-v02h03.nc"));

    }

//    @Test
//    public void name() throws Exception {
//        ModisGridInputFormat.TileLut tilesLut = getTilesLut(new File("c:\\ssd\\modis-tiles-lut2.txt"));
//        ArrayList<Integer> sizes = new ArrayList<>();
//        for (Set<String> strings : tilesLut.values()) {
//            sizes.add(strings.size());
//        }
//        Collections.sort(sizes);
//        System.out.println(sizes);
//    }

    static ModisGridInputFormat.TileLut getTilesLut(File modisTilesFile) {
        Gson gson = new Gson();
        ModisGridInputFormat.TileLut tileLut;
        try (FileReader fileReader = new FileReader(modisTilesFile)) {
            tileLut = gson.fromJson(fileReader, ModisGridInputFormat.TileLut.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return tileLut;
    }

}