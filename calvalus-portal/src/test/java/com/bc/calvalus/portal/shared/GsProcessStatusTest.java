package com.bc.calvalus.portal.shared;

import junit.framework.TestCase;

public class GsProcessStatusTest extends TestCase {

    public void testIsDone() {
        assertEquals(false, new GsProcessStatus(GsProcessState.SCHEDULED).isDone());
        assertEquals(false, new GsProcessStatus(GsProcessState.RUNNING).isDone());
        assertEquals(false, new GsProcessStatus(GsProcessState.UNKNOWN).isDone());
        assertEquals(true, new GsProcessStatus(GsProcessState.CANCELLED).isDone());
        assertEquals(true, new GsProcessStatus(GsProcessState.COMPLETED).isDone());
        assertEquals(true, new GsProcessStatus(GsProcessState.ERROR).isDone());
    }

    public void testEquals() {
        GsProcessStatus status = new GsProcessStatus();
        assertTrue(status.equals(status));
        assertTrue(status.equals(new GsProcessStatus()));
        assertTrue(new GsProcessStatus(GsProcessState.COMPLETED).equals(
                new GsProcessStatus(GsProcessState.COMPLETED)));
        assertTrue(new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.2f).equals(
                new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.2f)));
        assertTrue(new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.20001f).equals(
                new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.20003f)));
    }

    public void testNotEquals() {
        assertFalse(new GsProcessStatus(GsProcessState.ERROR, "", 0.1f).equals(
                new GsProcessStatus(GsProcessState.COMPLETED, "", 0.1f)));
        assertFalse(new GsProcessStatus(GsProcessState.RUNNING, "", 0.21f).equals(
                new GsProcessStatus(GsProcessState.RUNNING, "", 0.22f)));
        assertFalse(new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.2f).equals(
                new GsProcessStatus(GsProcessState.RUNNING, "Vorbei!", 0.2f)));
        assertFalse(new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.2f, 34).equals(
                new GsProcessStatus(GsProcessState.RUNNING, "Dabei!", 0.2f, 35)));
    }
}
