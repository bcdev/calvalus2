package com.bc.calvalus.processing.fire.format.grid.olci;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OlciGridMapperTest {

    @Test
    public void testGetTile() {
        assertEquals("h31v10", OlciGridMapper.getTile("hdfs://calvalus/calvalus/projects/fire/auxiliary/lc-v2.1.1-split-for-olci/lc-h31v10.nc"));
    }

    @Test
    public void testUntar_1() {
        File[] untaredFiles = null;
        List<String> newDirs = new ArrayList<>();
        try {
            File outputTarFile = new File(getClass().getResource("ba-outputs-h31v10-2018-01.tar.gz").toURI());
            untaredFiles = OlciGridMapper.untar(outputTarFile, "(.*Classification.*|.*Uncertainty.*)", newDirs);
            assertEquals(2, untaredFiles.length);
            assertTrue(untaredFiles[0].exists());
            assertTrue(untaredFiles[1].exists());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            cleanup(untaredFiles, newDirs);
        }
    }

    @Test
    public void testUntar_2() {
        File[] untaredFiles = null;
        List<String> newDirs = new ArrayList<>();
        try {
            File outputTarFile = new File(getClass().getResource("ba-composites-h31v10-2018-01.tar.gz").toURI());
            untaredFiles = OlciGridMapper.untar(outputTarFile, ".*FractionOfObservedArea.*", newDirs);
            assertEquals(1, untaredFiles.length);
            assertTrue(untaredFiles[0].exists());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            cleanup(untaredFiles, newDirs);
        }
    }

    private static void cleanup(File[] untaredFiles, List<String> newDirs) {
        for (File untaredFile : untaredFiles) {
            if (untaredFile != null && !untaredFile.delete()) {
                fail("Failed to remove untared file '" + untaredFiles[0]);
            }
        }
        for (int i = 0; i < newDirs.size(); i++) {
            // for the count of each new directory, build dir string and remove
            StringBuilder newDir = new StringBuilder("./");
            for (int j = 0; j < newDirs.size() - i; j++) {
                newDir.append(newDirs.get(j)).append("/");
            }
            if (!new File(newDir.toString()).delete()) {
                fail("Failed to remove untared dir '" + newDirs.get(i));
            }
        }
    }
}