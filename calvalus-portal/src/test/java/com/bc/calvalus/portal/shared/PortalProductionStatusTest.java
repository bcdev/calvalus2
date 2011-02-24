package com.bc.calvalus.portal.shared;

import com.google.gwt.junit.client.GWTTestCase;

public class PortalProductionStatusTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testIsDone() {
        assertEquals(false, new PortalProductionStatus(PortalProductionStatus.State.WAITING).isDone());
        assertEquals(false, new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionStatus.State.CANCELLED).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionStatus.State.COMPLETED).isDone());
        assertEquals(true, new PortalProductionStatus(PortalProductionStatus.State.ERROR).isDone());
    }

    public void testEquals() {
        PortalProductionStatus status = new PortalProductionStatus();
        assertTrue(status.equals(status));
        assertTrue(status.equals(new PortalProductionStatus()));
        assertTrue(new PortalProductionStatus(PortalProductionStatus.State.COMPLETED).equals(
                new PortalProductionStatus(PortalProductionStatus.State.COMPLETED)));
        assertTrue(new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Dabei!", 0.2f)));
        assertTrue(new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Dabei!", 0.20001f).equals(
                new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Dabei!", 0.20003f)));
    }

    public void testNotEquals() {
        assertFalse(new PortalProductionStatus(PortalProductionStatus.State.ERROR, "", 0.1f).equals(
                new PortalProductionStatus(PortalProductionStatus.State.COMPLETED, "", 0.1f)));
        assertFalse(new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "", 0.21f).equals(
                new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "", 0.22f)));
        assertFalse(new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Dabei!", 0.2f).equals(
                new PortalProductionStatus(PortalProductionStatus.State.IN_PROGRESS, "Vorbei!", 0.2f)));
    }
}
