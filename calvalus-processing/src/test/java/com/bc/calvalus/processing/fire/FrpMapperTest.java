package com.bc.calvalus.processing.fire;

import org.apache.hadoop.fs.Path;
import org.esa.snap.binning.BinningContext;
import org.esa.snap.binning.VariableContext;
import org.junit.Test;
import ucar.ma2.Array;
import ucar.ma2.DataType;

import java.io.IOException;
import java.util.HashMap;

import static com.bc.calvalus.processing.fire.FrpMapper.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class FrpMapperTest {

    @Test
    public void testCopyDateString() {
        final Path path = new Path("S3A_SL_1_RBT____20190911T001906_20190911T002206_20190912T040638_0179_049_116_2880_LN2_O_NT_003.zip");

        final String dateString = FrpMapper.copyDateString(path);
        assertEquals("20190911T001906", dateString);
    }

    @Test
    public void testCreateFiresLUT() {
        final int[] shape = {3};
        final Array j = Array.factory(DataType.INT, shape, new int[]{123, 136, 324});
        final Array i = Array.factory(DataType.SHORT, shape, new short[]{67, 78, 104});
        final Array[] frpArrays = new Array[FrpMapper.FRP_VARIABLES.values().length];
        frpArrays[FrpMapper.FRP_VARIABLES.j.ordinal()] = j;
        frpArrays[FrpMapper.FRP_VARIABLES.i.ordinal()] = i;
        final int numFires = 3;

        final HashMap<Integer, Integer> firesLUT = FrpMapper.createFiresLUT(frpArrays, numFires, 500);
        assertEquals(3, firesLUT.size());
        assertEquals(0, (int) firesLUT.get(123 * 500 + 67));
        assertEquals(1, (int) firesLUT.get(136 * 500 + 78));
        assertEquals(2, (int) firesLUT.get(324 * 500 + 104));
    }

    @Test
    public void testCreateVariableIndex() {
        final BinningContext binningContext = mock(BinningContext.class);
        final VariableContext variableContext = mock(VariableContext.class);
        when(variableContext.getVariableIndex("s3a_day_pixel")).thenReturn(0);
        when(variableContext.getVariableIndex("s3a_day_cloud")).thenReturn(1);
        when(variableContext.getVariableIndex("s3a_day_water")).thenReturn(2);
        when(variableContext.getVariableIndex("s3a_day_fire")).thenReturn(3);
        when(variableContext.getVariableIndex("s3a_day_frp")).thenReturn(4);
        when(variableContext.getVariableIndex("s3a_night_pixel")).thenReturn(5);
        when(variableContext.getVariableIndex("s3a_night_cloud")).thenReturn(6);
        when(variableContext.getVariableIndex("s3a_night_water")).thenReturn(7);
        when(variableContext.getVariableIndex("s3a_night_fire")).thenReturn(8);
        when(variableContext.getVariableIndex("s3a_night_frp")).thenReturn(9);
        when(variableContext.getVariableIndex("s3b_day_pixel")).thenReturn(10);
        when(variableContext.getVariableIndex("s3b_day_cloud")).thenReturn(11);
        when(variableContext.getVariableIndex("s3b_day_water")).thenReturn(12);
        when(variableContext.getVariableIndex("s3b_day_fire")).thenReturn(13);
        when(variableContext.getVariableIndex("s3b_day_frp")).thenReturn(14);
        when(variableContext.getVariableIndex("s3b_night_pixel")).thenReturn(15);
        when(variableContext.getVariableIndex("s3b_night_cloud")).thenReturn(16);
        when(variableContext.getVariableIndex("s3b_night_water")).thenReturn(17);
        when(variableContext.getVariableIndex("s3b_night_fire")).thenReturn(18);
        when(variableContext.getVariableIndex("s3b_night_frp")).thenReturn(19);
        when(variableContext.getVariableCount()).thenReturn(20);

        when(binningContext.getVariableContext()).thenReturn(variableContext);

        final int[] variableIndex = FrpMapper.createVariableIndex(binningContext, VARIABLE_NAMES);
        assertEquals(20, variableIndex.length);
        assertEquals(5, variableIndex[5]);
        assertEquals(13, variableIndex[13]);

        verify(binningContext, times(1)).getVariableContext();
        verify(variableContext, times(20)).getVariableIndex(anyString());
        verify(variableContext, times(1)).getVariableCount();
        verifyNoMoreInteractions(binningContext, variableContext);
    }

    @Test
    public void testCreateVariableIndex_monthly() {
        final BinningContext binningContext = mock(BinningContext.class);
        final VariableContext variableContext = mock(VariableContext.class);
        when(variableContext.getVariableIndex("fire_land_pixel")).thenReturn(7);
        when(variableContext.getVariableIndex("frp_mir_land_mean")).thenReturn(5);
        when(variableContext.getVariableIndex("fire_water_pixel")).thenReturn(4);
        when(variableContext.getVariableIndex("pixel")).thenReturn(3);
        when(variableContext.getVariableIndex("water_pixel")).thenReturn(2);
        when(variableContext.getVariableIndex("cloud_land_pixel")).thenReturn(1);
        when(variableContext.getVariableCount()).thenReturn(6);

        when(binningContext.getVariableContext()).thenReturn(variableContext);

        final int[] variableIndex = FrpMapper.createVariableIndex(binningContext, VARIABLE_NAMES_MONTHLY);
        assertEquals(6, variableIndex.length);
        assertEquals(4, variableIndex[1]);
        assertEquals(2, variableIndex[4]);

        verify(binningContext, times(1)).getVariableContext();
        verify(variableContext, times(6)).getVariableIndex(anyString());
        verify(variableContext, times(1)).getVariableCount();
        verifyNoMoreInteractions(binningContext, variableContext);
    }

    @Test
    public void testCreateVariableIndex_invalidNumber() {
        final BinningContext binningContext = mock(BinningContext.class);
        final VariableContext variableContext = mock(VariableContext.class);
        when(variableContext.getVariableCount()).thenReturn(16);
        when(binningContext.getVariableContext()).thenReturn(variableContext);

        try {
            FrpMapper.createVariableIndex(binningContext, VARIABLE_NAMES);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testCreateVariableIndex_variables_missing() {
        final BinningContext binningContext = mock(BinningContext.class);
        final VariableContext variableContext = mock(VariableContext.class);
        when(variableContext.getVariableIndex("s3a_day_pixel")).thenReturn(0);
        when(variableContext.getVariableIndex("s3a_day_cloud")).thenReturn(1);
        when(variableContext.getVariableIndex("s3a_day_water")).thenReturn(-1); // <--- here!
        when(variableContext.getVariableIndex("s3a_day_fire")).thenReturn(3);
        when(variableContext.getVariableIndex("s3a_day_frp")).thenReturn(4);
        when(variableContext.getVariableIndex("s3a_night_pixel")).thenReturn(5);
        when(variableContext.getVariableIndex("s3a_night_cloud")).thenReturn(6);
        when(variableContext.getVariableIndex("s3a_night_water")).thenReturn(7);
        when(variableContext.getVariableIndex("s3a_night_fire")).thenReturn(8);
        when(variableContext.getVariableIndex("s3a_night_frp")).thenReturn(9);
        when(variableContext.getVariableIndex("s3b_day_pixel")).thenReturn(10);
        when(variableContext.getVariableIndex("s3b_day_cloud")).thenReturn(11);
        when(variableContext.getVariableIndex("s3b_day_water")).thenReturn(12);
        when(variableContext.getVariableIndex("s3b_day_fire")).thenReturn(13);
        when(variableContext.getVariableIndex("s3b_day_frp")).thenReturn(-1); // <--- here!
        when(variableContext.getVariableIndex("s3b_night_pixel")).thenReturn(15);
        when(variableContext.getVariableIndex("s3b_night_cloud")).thenReturn(16);
        when(variableContext.getVariableIndex("s3b_night_water")).thenReturn(17);
        when(variableContext.getVariableIndex("s3b_night_fire")).thenReturn(18);
        when(variableContext.getVariableIndex("s3b_night_frp")).thenReturn(19);
        when(variableContext.getVariableCount()).thenReturn(20);

        when(binningContext.getVariableContext()).thenReturn(variableContext);

        try {
            FrpMapper.createVariableIndex(binningContext, VARIABLE_NAMES);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testGetSensorOffset() {
        assertEquals(0, FrpMapper.getSensorOffset(1));
        assertEquals(10, FrpMapper.getSensorOffset(2));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testGetSensorOffset_invalid() {
        try {
            FrpMapper.getSensorOffset(98);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testGetPlatformNumber() {
        Path path = new Path("S3A_SL_1_RBT____20190911T001906_20190911T002206_20190912T040638_0179_049_116_2880_LN2_O_NT_003.zip");
        assertEquals(1, FrpMapper.getPlatformNumber(path));

        path = new Path("S3B_SL_1_RBT____20190911T001906_20190911T002206_20190912T040638_0179_049_116_2880_LN2_O_NT_003.zip");
        assertEquals(2, FrpMapper.getPlatformNumber(path));
    }

    @Test
    public void testGetPlatformNumber_illegal() {
        try {
            FrpMapper.getPlatformNumber(new Path("Rapunzel"));
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    public void testGetTimeRange() throws IOException {
        final long[] range = FrpMapper.getTimeRange("[2020-07-01:2020-07-31]");
        assertEquals(2, range.length);
        assertEquals(646876800000000L, range[0]);
        assertEquals(649555199999000L, range[1]);
    }

    @Test
    public void testIsCloud() {
        assertEquals(1, FrpMapper.isCloud(FRP_CLOUD));
        assertEquals(1, FrpMapper.isCloud(FRP_CLOUD + 19));

        assertEquals(0, FrpMapper.isCloud(0));
        assertEquals(0, FrpMapper.isCloud(19));
    }

    @Test
    public void testIsUnfilled() {
        assertTrue(FrpMapper.isUnfilled(CONF_IN_UNFILLED));
        assertTrue(FrpMapper.isUnfilled(CONF_IN_UNFILLED + 17));

        assertFalse(FrpMapper.isUnfilled(0));
        assertFalse(FrpMapper.isUnfilled(1024));
    }

    @Test
    public void testIsWater() {
        assertTrue(FrpMapper.isWater(FRP_WATER));
        assertTrue(FrpMapper.isWater(L1B_WATER));

        assertFalse(FrpMapper.isWater(0));
        assertFalse(FrpMapper.isWater(CONF_IN_UNFILLED));
        assertFalse(FrpMapper.isWater(FRP_CLOUD));
    }

    @Test
    public void testIsDay() {
        assertTrue(FrpMapper.isDay(DAY));

        assertFalse(FrpMapper.isDay(0));
        assertFalse(FrpMapper.isDay(L1B_WATER));
        assertFalse(FrpMapper.isDay(FRP_CLOUD));
    }
}
