package com.bc.calvalus.portal.client;


import com.bc.calvalus.portal.shared.WorkStatus;
import com.google.gwt.user.client.Timer;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class that periodically notifies work observers if new work status is reported.
 *
 * @author Norman
 */
public final class Worker {

    private final WorkReporter reporter;
    private final List<WorkObserver> observerList;

    private Timer timer;
    boolean observed;

    public Worker(WorkReporter reporter) {
        this.reporter = reporter;
        this.observerList = new ArrayList<WorkObserver>();
    }

    public Worker(WorkReporter reporter, WorkObserver observer) {
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

    public boolean isDone() {
        return timer == null;
    }

    public void start(int updatePeriod) {
        WorkStatus workStatus = reporter.getWorkStatus();
        if (!workStatus.isDone()) {
            this.timer = new Timer() {
                @Override
                public void run() {
                    update();
                }
            };
            this.timer.scheduleRepeating(updatePeriod);
        }
    }

    private void update() {
        WorkObserver[] observers = getObservers();
        WorkStatus workStatus = reporter.getWorkStatus();
        if (workStatus.isDone()) {
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
            if (observed) {
                for (WorkObserver observer : observers) {
                    observer.workDone(workStatus);
                }
                observed = false;
            }
        } else {
            if (!observed) {
                for (WorkObserver observer : observers) {
                    observer.workStarted(workStatus);
                }
            }
            for (WorkObserver observer : observers) {
                observer.workProgressing(workStatus);
            }
            observed = true;
        }
    }

}