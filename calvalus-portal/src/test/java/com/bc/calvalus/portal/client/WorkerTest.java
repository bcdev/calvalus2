package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.Timer;

public class WorkerTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testNothing() {
    }

    public void it_does_not_work_testStatusJustStarted() {
        MyStatus status = new MyStatus(0);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        delayTestFinish(10000);
        worker.start(10);
    }

    public void it_does_not_work_testStatusPartlyDone() {
        MyStatus status = new MyStatus(50);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        delayTestFinish(10000);
        worker.start(10);
    }

    public void it_does_not_work_testStatusIsAlreadyDone() {
        MyStatus status = new MyStatus(100);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        worker.start(10);
        delayTestFinish(3000);
        assertEquals(0, observer.updates);
    }

    private static class MyStatus implements WorkReporter {
        int percent;

        private MyStatus(int percent) {
            this.percent = percent;
            Timer t = new Timer() {
                @Override
                public void run() {
                    MyStatus.this.percent++;
                }
            };
            t.scheduleRepeating(10);
        }


        @Override
        public WorkStatus getWorkStatus() {
            return new WorkStatus(WorkStatus.State.IN_PROGRESS, "" + percent, percent / 100.0);
        }
    }

    private class MyObserver implements WorkObserver {
        int updates = 0;

        @Override
        public void workStarted(WorkStatus work) {
            assertEquals(0, updates);
            updates = 1;
        }

        @Override
        public void workProgressing(WorkStatus work) {
            updates++;
        }

        @Override
        public void workStopped(WorkStatus work) {
            assertTrue(updates > 1);
            finishTest();
        }
    }
}
