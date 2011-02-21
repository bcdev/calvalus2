package com.bc.calvalus.portal.client;


import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.core.client.GWT;
import com.google.gwt.user.client.Timer;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class that periodically notifies work observers if new work status is reported.
 *
 * @author Norman
 */
public final class WorkMonitor {

    private final WorkReporter reporter;
    private final List<WorkObserver> observerList;

    private Timer timer;
    boolean firedStart;

    public WorkMonitor(WorkReporter reporter) {
        this.reporter = reporter;
        this.observerList = new ArrayList<WorkObserver>();
    }

    public WorkMonitor(WorkReporter reporter, WorkObserver observer) {
        this(reporter);
        addObserver(observer);
    }

    public WorkReporter getReporter() {
        return reporter;
    }

    public void addObserver(WorkObserver observer) {
        this.observerList.add(observer);
    }

    public void removeObserver(WorkObserver observer) {
        this.observerList.remove(observer);
    }

    public WorkObserver[] getObservers() {
        return observerList.toArray(new WorkObserver[observerList.size()]);
    }

    public void start(int updatePeriod) {
        WorkStatus workStatus = reporter.getWorkStatus();
        if (!workStatus.isDone()) {
            GWT.log("Worker monitor started for reporter " + reporter + ". Updating all " + updatePeriod + " ms");
            this.timer = new Timer() {
                @Override
                public void run() {
                    update();
                }
            };
            this.timer.scheduleRepeating(updatePeriod);
        } else {
            GWT.log("Worker monitor NOT started for reporter " + reporter + " because it's already done.");
        }
    }

    private void update() {
        WorkStatus workStatus = reporter.getWorkStatus();
        WorkObserver[] observers = getObservers();
        if (workStatus.isDone()) {
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
            if (firedStart) {
                for (WorkObserver observer : observers) {
                    observer.workStopped(workStatus);
                }
                firedStart = false;
            }
        } else {
            if (!firedStart) {
                for (WorkObserver observer : observers) {
                    observer.workStarted(workStatus);
                }
                firedStart = true;
            }
            // todo - only fire progress events, if status really has changed
            for (WorkObserver observer : observers) {
                observer.workProgressing(workStatus);
            }
        }
    }

}