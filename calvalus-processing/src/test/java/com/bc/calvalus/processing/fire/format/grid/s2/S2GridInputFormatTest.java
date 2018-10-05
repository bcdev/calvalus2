package com.bc.calvalus.processing.fire.format.grid.s2;

import com.bc.calvalus.processing.fire.format.S2Strategy;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S2GridInputFormatTest {

    @Test
    public void testNameMatches() throws Exception {
        assertTrue("hdfs://calvalus/calvalus/projects/fire/s2-ba/T28PHU/BA-T28PHU-20160527T112220.nc".matches(
                "hdfs://calvalus/calvalus/projects/fire/s2-ba/(T27NYB|T28PHU)/BA-.*201605.*nc"));

    }

    @Test
    public void testGetAreas() {
        S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] areas = S2GridInputFormat.getAreas("x210y94");
        List<S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea> areaList = Arrays.asList(areas);
        assertEquals(1, areaList.size());
        assertTrue(areaList.contains(S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea.h42v17));
    }

//    @Test
//    public void testGetAreas_2() {
//        S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea[] areas = S2GridInputFormat.getAreas("x160y102");
//        List<S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea> areaList = Arrays.asList(areas);
//        assertEquals(1, areaList.size());
//        assertTrue(areaList.contains(S2Strategy.S2PixelProductAreaProvider.S2PixelProductArea.h32v15));
//    }
}