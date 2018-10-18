package com.bc.calvalus.processing.fire.format.grid;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static com.bc.calvalus.processing.fire.format.grid.GridFormatUtils.make2Dims;
import static org.junit.Assert.assertEquals;

public class AbstractFireGridDataSourceTest {

    private AbstractFireGridDataSource dataSource;

    @Before
    public void setUp() throws Exception {
        dataSource = new AbstractFireGridDataSource(4, 4) {
            @Override
            public SourceData readPixels(int x, int y) throws IOException {
                return null;
            }
        };
        dataSource.setDoyFirstOfMonth(1);
        dataSource.setDoyLastOfMonth(31);
    }

    @Test
    public void testGetPatches_0() throws Exception {
        float[] pixels = {
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0,
                0, 0, 0, 0
        };

        boolean[] burnable = {
                true, true, true, true,
                true, true, true, true,
                true, true, true, true,
                true, true, true, true
        };

        assertEquals(0, dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable)));
    }

    @Test
    public void testGetPatches_1() throws Exception {
        float[] pixels = {
                0, 0, 0, 1,
                0, 0, 0, 1,
                1, 0, 0, 0,
                1, 0, 0, 1
        };
        boolean[] burnable = {
                true, true, true, true,
                true, true, true, true,
                true, true, true, true,
                true, true, true, true
        };
        assertEquals(3, dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable)));
    }

    @Test
    public void testGetPatches_Not_all_burnable() throws Exception {
        float[] pixels = {
                0, 0, 0, 1,
                0, 0, 0, 1,
                1, 0, 0, 0,
                1, 0, 0, 1
        };
        boolean[] burnable = {
                true, true, true, false,
                true, true, true, false,
                true, true, true, true,
                true, true, true, true
        };
        assertEquals(2, dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable)));
    }

    @Test
    public void testGetPatches_2() throws Exception {
        float[] pixels = {
                0, 0, 0, 0,
                0, 1, 0, 0,
                0, 1, 1, 0,
                0, 0, 0, 0
        };
        boolean[] burnable = {
                true, true, true, true,
                true, true, true, true,
                true, true, true, true,
                true, true, true, true
        };
        assertEquals(1, dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable)));
    }

    @Test
    public void testGetPatches_3() throws Exception {
        float[] pixels = {
                1, 0, 1, 1,
                0, 1, 0, 1,
                0, 1, 1, 0,
                0, 0, 0, 1
        };
        boolean[] burnable = {
                true, true, true, true,
                true, true, true, true,
                true, true, true, true,
                true, true, true, true
        };
        assertEquals(4, dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable)));
    }

    @Test
    public void testGetPatches_Large() {
        float[] pixels = new float[90 * 90];
        for (int i = 0; i < pixels.length; i++) {
            pixels[i] = (int) (Math.random() * 2);
        }
        boolean[] burnable = new boolean[90 * 90];
        Arrays.fill(burnable, true);
        dataSource.getPatchNumbers(make2Dims(pixels), GridFormatUtils.make2Dims(burnable));
    }
}