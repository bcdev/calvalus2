package com.bc.calvalus.processing.fire.format.pixel.s2;

import org.esa.snap.core.datamodel.GeoPos;
import org.junit.Test;

import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.CL;
import static com.bc.calvalus.processing.fire.format.pixel.PixelFinaliseMapper.JD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class S2PixelFinaliseMapperTest {

    @Test
    public void testGetTilesForGeoPos() throws Exception {
        S2PixelFinaliseMapper.S2NanHandler s2NaNHandler = new S2PixelFinaliseMapper.S2NanHandler(null);
        assertArrayEquals(new String[]{"32PRS"}, s2NaNHandler.getTilesForGeoPos(new GeoPos(10.0, 12.0)));
        assertArrayEquals(new String[]{"32PRR", "32PRS"}, s2NaNHandler.getTilesForGeoPos(new GeoPos(9.88, 12.0958)));
        assertArrayEquals(new String[]{"34HDK"}, s2NaNHandler.getTilesForGeoPos(new GeoPos(-32, 20.62)));
    }

    @Test
    public void testPreferTo() throws Exception {
        assertEquals(-2, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-2, -2, JD));
        assertEquals(-2, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-2, -1, JD));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-2, 0, JD));
        assertEquals(-2, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-1, -2, JD));
        assertEquals(-1, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-1, -1, JD));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(-1, 0, JD));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(0, -2, JD));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(0, -1, JD));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(0, 0, JD));

        assertEquals(1, S2PixelFinaliseMapper.S2NanHandler.preferredValue(0, 1, CL));
        assertEquals(1, S2PixelFinaliseMapper.S2NanHandler.preferredValue(1, 0, CL));
        assertEquals(0, S2PixelFinaliseMapper.S2NanHandler.preferredValue(0, 0, CL));
        assertEquals(1, S2PixelFinaliseMapper.S2NanHandler.preferredValue(1, 1, CL));
    }
}