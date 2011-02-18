package com.bc.calvalus.portal.client;

import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.junit.client.GWTTestCase;
import com.google.gwt.user.client.Timer;

public class WorkerTest extends GWTTestCase {

    @Override
    public String getModuleName() {
        return "com.bc.calvalus.portal.CalvalusPortalJUnit";
    }

    public void testStatusJustStarted() {
        MyStatus status = new MyStatus(0);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        worker.start(10);
        sleep(2000);
        assertEquals("start;stop;", observer.trace);
        assertTrue(observer.updates > 10);
    }

    public void testStatusPartlyDone() {
        MyStatus status = new MyStatus(50);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        worker.start(10);
        sleep(2000);
        assertEquals("start;stop;", observer.trace);
        assertTrue(observer.updates > 0);
    }

    public void testStatusIsAlreadyDone() {
        MyStatus status = new MyStatus(100);
        MyObserver observer = new MyObserver();
        Worker worker = new Worker(status, observer);
        worker.start(10);
        sleep(2000);
        assertEquals("", observer.trace);
        assertEquals(0, observer.updates);
    }

    void sleep(long delta) {
        long t0 = System.currentTimeMillis();
        long t1 = t0 + delta;
        while (System.currentTimeMillis() < t1) {
        }
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

    private static class MyObserver implements WorkObserver {
        String trace = "";
        int updates = 0;

        @Override
        public void workStarted(WorkStatus work) {
            trace += "started;";
        }

        @Override
        public void workProgressing(WorkStatus work) {
            updates++;
        }

        @Override
        public void workDone(WorkStatus work) {
            trace += "stopped;";
        }
    }
}
