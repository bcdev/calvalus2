package com.bc.calvalus.portal.shared;

import com.google.gwt.junit.client.GWTTestCase;

public class PortalProductionStatusTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testIsDone() {
        assertEquals(false, new PortalProductionStatus(PortalProductionState.WAITING).isDone());
        assertEquals(false, new PortalProductionStatus(PortalProductionState.IN_PROGRESS).isDone());
        assertEquals(false, new PortalProductionStatus(PortalProductionState.UNKNOWN).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionState.CANCELLED).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionState.COMPLETED).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionState.ERROR).isDone());
    }

    public void testEquals() {
        PortalProductionStatus status = new PortalProductionStatus();
        assertTrue(status.equals(status));
        assertTrue(status.equals(new PortalProductionStatus()));
        assertTrue(new PortalProductionStatus(PortalProductionState.COMPLETED).equals(
                new PortalProductionStatus(PortalProductionState.COMPLETED)));
        assertTrue(new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Dabei!", 0.2f)));
        assertTrue(new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Dabei!", 0.20001f).equals(
                new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Dabei!", 0.20003f)));
    }

    public void testNotEquals() {
        assertFalse(new PortalProductionStatus(PortalProductionState.ERROR, "", 0.1f).equals(
                new PortalProductionStatus(PortalProductionState.COMPLETED, "", 0.1f)));
        assertFalse(new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "", 0.21f).equals(
                new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "", 0.22f)));
        assertFalse(new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new PortalProductionStatus(PortalProductionState.IN_PROGRESS, "Vorbei!", 0.2f)));
    }
}
