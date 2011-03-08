package com.bc.calvalus.commons;

import org.junit.Test;

import static org.junit.Assert.*;

public class ProcessStatusTest {

    @Test
    public void testConstructors() {
        ProcessStatus unknown = ProcessStatus.UNKNOWN;
        assertEquals(ProcessState.UNKNOWN, unknown.getState());
        assertEquals("", unknown.getMessage());
        assertEquals(0.0f, unknown.getProgress(), 1e-5);

        ProcessStatus done = new ProcessStatus(ProcessState.COMPLETED, 1.0f);
        assertEquals(ProcessState.COMPLETED, done.getState());
        assertEquals("", done.getMessage());
        assertEquals(1.0f, done.getProgress(), 1e-5);

        ProcessStatus error = new ProcessStatus(ProcessState.ERROR, 0.0f, "File not found");
        assertEquals(ProcessState.ERROR, error.getState());
        assertEquals("File not found", error.getMessage());
        assertEquals(0.0f, error.getProgress(), 1e-5);

        try {
            new ProcessStatus(null, 0.0f, "File not found");
            fail("State must not be null");
        } catch (NullPointerException e) {
            // ok
        }

        try {
            new ProcessStatus(ProcessState.COMPLETED, 0.0f, null);
            fail("State must not be null");
        } catch (NullPointerException e) {
            // ok
        }
    }


    @Test
    public void testAggregate() {
        assertEquals(null,
                     ProcessStatus.aggregate());
        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f, "Hello!"),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f, "Hello!")));
        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.3f, ""),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, ""),
                                             new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f, ""),
                                             new ProcessStatus(ProcessState.IN_PROGRESS, 0.3f, "")));
        assertEquals(new ProcessStatus(ProcessState.IN_PROGRESS, 0.8f, "Wait"),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.IN_PROGRESS, 0.4f, "Wait"),
                                             new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""),
                                             new ProcessStatus(ProcessState.COMPLETED, 1.0f, "")));
        assertEquals(new ProcessStatus(ProcessState.COMPLETED, 1.0f, "This was hard"),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""),
                                             new ProcessStatus(ProcessState.COMPLETED, 1.0f, "This was hard"),
                                             new ProcessStatus(ProcessState.COMPLETED, 1.0f, "")));
        assertEquals(new ProcessStatus(ProcessState.ERROR, 0.8f, "I/O problem"),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.COMPLETED, 1.0f, ""),
                                             new ProcessStatus(ProcessState.ERROR, 0.4f, "I/O problem"),
                                             new ProcessStatus(ProcessState.COMPLETED, 1.0f, "")));
        assertEquals(new ProcessStatus(ProcessState.CANCELLED, 0.3f, "Go away"),
                     ProcessStatus.aggregate(new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, ""),
                                             new ProcessStatus(ProcessState.IN_PROGRESS, 0.6f, ""),
                                             new ProcessStatus(ProcessState.CANCELLED, 0.1f, "Go away")));
    }

    @Test
    public void testIsDone() {
        assertEquals(true, new ProcessStatus(ProcessState.COMPLETED).isDone());
        assertEquals(true, new ProcessStatus(ProcessState.ERROR).isDone());
        assertEquals(true, new ProcessStatus(ProcessState.CANCELLED).isDone());

        assertEquals(false, new ProcessStatus(ProcessState.UNKNOWN).isDone());
        assertEquals(false, new ProcessStatus(ProcessState.IN_PROGRESS).isDone());
        assertEquals(false, new ProcessStatus(ProcessState.WAITING).isDone());
    }

    @Test
    public void testEquals() {
        ProcessStatus status = ProcessStatus.UNKNOWN;
        assertTrue(status.equals(status));
        assertTrue(status.equals(new ProcessStatus(ProcessState.UNKNOWN)));
        assertTrue(new ProcessStatus(ProcessState.COMPLETED).equals(
                new ProcessStatus(ProcessState.COMPLETED)));
        assertTrue(new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, "Dabei!").equals(
                new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, "Dabei!")));
        assertTrue(new ProcessStatus(ProcessState.IN_PROGRESS, 0.20001f, "Dabei!").equals(
                new ProcessStatus(ProcessState.IN_PROGRESS, 0.20003f, "Dabei!")));
    }

    @Test
    public void testNotEquals() {
        assertFalse(new ProcessStatus(ProcessState.ERROR, 0.1f, "").equals(
                new ProcessStatus(ProcessState.COMPLETED, 0.1f, "")));
        assertFalse(new ProcessStatus(ProcessState.IN_PROGRESS, 0.21f, "").equals(
                new ProcessStatus(ProcessState.IN_PROGRESS, 0.22f, "")));
        assertFalse(new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, "Dabei!").equals(
                new ProcessStatus(ProcessState.IN_PROGRESS, 0.2f, "Vorbei!")));
    }
}
