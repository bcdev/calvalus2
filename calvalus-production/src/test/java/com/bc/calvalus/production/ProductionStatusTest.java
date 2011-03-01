package com.bc.calvalus.production;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProductionStatusTest {

    @Test
    public void testConstructors() {
        ProductionStatus unknown = new ProductionStatus();
        assertEquals(ProductionState.UNKNOWN, unknown.getState());
        assertEquals(null, unknown.getMessage());
        assertEquals(0.0f, unknown.getProgress(), 1e-5);

        ProductionStatus done = new ProductionStatus(ProductionState.COMPLETED, null, 1.0f);
        assertEquals(ProductionState.COMPLETED, done.getState());
        assertEquals(null, done.getMessage());
        assertEquals(1.0f, done.getProgress(), 1e-5);

        ProductionStatus error = new ProductionStatus(ProductionState.ERROR, "File not found", 0.0f);
        assertEquals(ProductionState.ERROR, error.getState());
        assertEquals("File not found", error.getMessage());
        assertEquals(0.0f, error.getProgress(), 1e-5);

        try {
            new ProductionStatus(null, "File not found", 0.0f);
            fail("State must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }


    @Test
    public void testIsDone() {
        assertEquals(true, new ProductionStatus(ProductionState.COMPLETED).isDone());
        assertEquals(true, new ProductionStatus(ProductionState.ERROR).isDone());
        assertEquals(true, new ProductionStatus(ProductionState.CANCELLED).isDone());

        assertEquals(false, new ProductionStatus(ProductionState.UNKNOWN).isDone());
        assertEquals(false, new ProductionStatus(ProductionState.IN_PROGRESS).isDone());
        assertEquals(false, new ProductionStatus(ProductionState.WAITING).isDone());
    }

    @Test
    public void testEquals() {
        ProductionStatus status = new ProductionStatus();
        assertTrue(status.equals(status));
        assertTrue(status.equals(new ProductionStatus()));
        assertTrue(new ProductionStatus(ProductionState.COMPLETED).equals(
                new ProductionStatus(ProductionState.COMPLETED)));
        assertTrue(new ProductionStatus(ProductionState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new ProductionStatus(ProductionState.IN_PROGRESS, "Dabei!", 0.2f)));
        assertTrue(new ProductionStatus(ProductionState.IN_PROGRESS, "Dabei!", 0.20001f).equals(
                new ProductionStatus(ProductionState.IN_PROGRESS, "Dabei!", 0.20003f)));
    }

    @Test
    public void testNotEquals() {
        assertFalse(new ProductionStatus(ProductionState.ERROR, "", 0.1f).equals(
                new ProductionStatus(ProductionState.COMPLETED, "", 0.1f)));
        assertFalse(new ProductionStatus(ProductionState.IN_PROGRESS, "", 0.21f).equals(
                new ProductionStatus(ProductionState.IN_PROGRESS, "", 0.22f)));
        assertFalse(new ProductionStatus(ProductionState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new ProductionStatus(ProductionState.IN_PROGRESS, "Vorbei!", 0.2f)));
    }
}
