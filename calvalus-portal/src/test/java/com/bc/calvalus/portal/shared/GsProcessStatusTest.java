package com.bc.calvalus.portal.shared;

import com.google.gwt.junit.client.GWTTestCase;

public class GsProcessStatusTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testIsDone() {
        assertEquals(false, new GsProcessStatus(GsProcessState.WAITING).isDone());
        assertEquals(false, new GsProcessStatus(GsProcessState.IN_PROGRESS).isDone());
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
        assertTrue(new GsProcessStatus(GsProcessState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new GsProcessStatus(GsProcessState.IN_PROGRESS, "Dabei!", 0.2f)));
        assertTrue(new GsProcessStatus(GsProcessState.IN_PROGRESS, "Dabei!", 0.20001f).equals(
                new GsProcessStatus(GsProcessState.IN_PROGRESS, "Dabei!", 0.20003f)));
    }

    public void testNotEquals() {
        assertFalse(new GsProcessStatus(GsProcessState.ERROR, "", 0.1f).equals(
                new GsProcessStatus(GsProcessState.COMPLETED, "", 0.1f)));
        assertFalse(new GsProcessStatus(GsProcessState.IN_PROGRESS, "", 0.21f).equals(
                new GsProcessStatus(GsProcessState.IN_PROGRESS, "", 0.22f)));
        assertFalse(new GsProcessStatus(GsProcessState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new GsProcessStatus(GsProcessState.IN_PROGRESS, "Vorbei!", 0.2f)));
    }
}
