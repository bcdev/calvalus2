package com.bc.calvalus.processing.fire.format.grid.olci;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class OlciGridReducerTest {

    private OlciGridReducer olciGridReducer;

    @Before
    public void setUp() throws Exception {
        olciGridReducer = new OlciGridReducer();
    }

    @Test
    public void getX() {
        // 0-1439
        assertEquals(400, olciGridReducer.getX("2018-01-h10v08"));
        assertEquals(1000, olciGridReducer.getX("2018-01-h25v02"));

    }

    @Test
    public void getY() {
        assertEquals(320, olciGridReducer.getY("2018-01-h10v08"));
        assertEquals(80, olciGridReducer.getY("2018-01-h25v02"));
    }
}