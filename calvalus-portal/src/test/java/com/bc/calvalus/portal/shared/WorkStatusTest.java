package com.bc.calvalus.portal.shared;

import com.bc.calvalus.portal.client.WorkObserver;
import com.bc.calvalus.portal.client.WorkReporter;
import com.bc.calvalus.portal.client.Worker;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.Timer;

public class WorkStatusTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testIsDone() {
        assertEquals(false, new WorkStatus(WorkStatus.State.WAITING, "", 0).isDone());
        assertEquals(false, new WorkStatus(WorkStatus.State.IN_PROGRESS, "", 0).isDone());
        assertEquals(true, new WorkStatus(WorkStatus.State.CANCELLED, "", 0).isDone());
        assertEquals(true, new WorkStatus(WorkStatus.State.DONE, "", 0).isDone());
        assertEquals(true, new WorkStatus(WorkStatus.State.ERROR, "", 0).isDone());
    }
}
