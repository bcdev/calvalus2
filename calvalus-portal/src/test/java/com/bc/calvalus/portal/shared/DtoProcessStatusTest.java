package com.bc.calvalus.portal.shared;

import junit.framework.TestCase;

public class DtoProcessStatusTest extends TestCase {

    public void testDefaultConstructorForGWTSerialisation() {
        DtoProcessStatus processStatus = new DtoProcessStatus();
        assertEquals("", processStatus.getMessage());
        assertEquals(0, processStatus.getProcessingSeconds());
        assertEquals(DtoProcessState.UNKNOWN, processStatus.getState());
        assertEquals(0.0F, processStatus.getProgress(), 1e-5F);
    }

    public void testIsDone() {
        assertEquals(false, new DtoProcessStatus(DtoProcessState.SCHEDULED).isDone());
        assertEquals(false, new DtoProcessStatus(DtoProcessState.RUNNING).isDone());
        assertEquals(false, new DtoProcessStatus(DtoProcessState.UNKNOWN).isDone());
        assertEquals(true, new DtoProcessStatus(DtoProcessState.CANCELLED).isDone());
        assertEquals(true, new DtoProcessStatus(DtoProcessState.COMPLETED).isDone());
        assertEquals(true, new DtoProcessStatus(DtoProcessState.ERROR).isDone());
    }

    public void testEquals() {
        DtoProcessStatus status = new DtoProcessStatus();
        assertTrue(status.equals(status));
        assertTrue(status.equals(new DtoProcessStatus()));
        assertTrue(new DtoProcessStatus(DtoProcessState.COMPLETED).equals(
                new DtoProcessStatus(DtoProcessState.COMPLETED)));
        assertTrue(new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.2f).equals(
                new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.2f)));
        assertTrue(new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.20001f).equals(
                new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.20003f)));
    }

    public void testNotEquals() {
        assertFalse(new DtoProcessStatus(DtoProcessState.ERROR, "", 0.1f).equals(
                new DtoProcessStatus(DtoProcessState.COMPLETED, "", 0.1f)));
        assertFalse(new DtoProcessStatus(DtoProcessState.RUNNING, "", 0.21f).equals(
                new DtoProcessStatus(DtoProcessState.RUNNING, "", 0.22f)));
        assertFalse(new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.2f).equals(
                new DtoProcessStatus(DtoProcessState.RUNNING, "Vorbei!", 0.2f)));
        assertFalse(new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.2f, 34).equals(
                new DtoProcessStatus(DtoProcessState.RUNNING, "Dabei!", 0.2f, 35)));
    }
}
