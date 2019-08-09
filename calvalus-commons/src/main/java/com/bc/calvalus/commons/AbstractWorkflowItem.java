/*
 * Copyright (C) 2010 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.commons;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Base class for workflow item implementations.
 * <p/>
 * Clients must implement {@link #submit()}, {@link #kill()} and {@link #updateStatus()} ()}.
 *
 * @author Norman
 */
public abstract class AbstractWorkflowItem implements WorkflowItem, WorkflowStatusListener {
    private static final WorkflowItem[] NO_ITEMS = new WorkflowItem[0];
    private final List<WorkflowStatusListener> statusListeners;
    private ProcessStatus status;
    private Date submitTime;
    private Date startTime;
    private Date stopTime;

    public AbstractWorkflowItem() {
        this(ProcessStatus.UNKNOWN);
    }

    protected AbstractWorkflowItem(ProcessStatus status) {
        this.status = status;
        this.statusListeners = new ArrayList<WorkflowStatusListener>();
        this.statusListeners.add(this);
    }

    @Override
    public void addWorkflowStatusListener(WorkflowStatusListener l) {
        statusListeners.add(l);
    }

    /**
     * @return the current status.
     */
    @Override
    public final ProcessStatus getStatus() {
        return status;
    }

    /**
     * Sets the new status.
     *
     * @param newStatus The new status.
     * @throws NullPointerException if newStatus is null.
     */
    @Override
    public final void setStatus(ProcessStatus newStatus) {
        if (!newStatus.equals(status) && !newStatus.equals(ProcessStatus.UNKNOWN)) {
            ProcessStatus oldStatus = status;
            status = newStatus;
            fireStatusChanged(new WorkflowStatusEvent(this, oldStatus, newStatus, new Date()));
        }
    }

    /**
     * @return The default implementation returns an empty array.
     */
    @Override
    public WorkflowItem[] getItems() {
        return NO_ITEMS;
    }

    /**
     * @return The default implementation returns an empty array.
     */
    @Override
    public Object[] getJobIds() {
        return new Object[0];
    }

    @Override
    public void handleStatusChanged(WorkflowStatusEvent event) {
        setTimesOnStateTransitionEvent(event);
    }

    private void setTimesOnStateTransitionEvent(WorkflowStatusEvent event) {
        ProcessState oldState = event.getOldStatus().getState();
        ProcessState newState = event.getNewStatus().getState();
        if (oldState != newState && event.getSource() == this) {
            if (newState == ProcessState.SCHEDULED) {
                setSubmitTime(event.getTime());
            } else if (newState == ProcessState.RUNNING) {
                setStartTime(event.getTime());
            } else if (newState.isDone()) {
                setStopTime(event.getTime());
            }
        }
    }

    protected void fireStatusChanged(WorkflowStatusEvent event) {
        for (WorkflowStatusListener changeListener : statusListeners) {
            changeListener.handleStatusChanged(event);
        }
    }

    /**
     * @return The workflow item's submit time, or {@code null} if unknown.
     */
    @Override
    public Date getSubmitTime() {
        return submitTime;
    }

    /**
     * @return The workflow item's start time, or {@code null} if unknown.
     */
    @Override
    public Date getStartTime() {
        return startTime;
    }

    /**
     * @return The workflow item's stop time, or {@code null} if unknown.
     */
    @Override
    public Date getStopTime() {
        return stopTime;
    }

    /**
     * Sets the submit time.
     * Called by {@link #handleStatusChanged(WorkflowStatusEvent)} whose call is triggered by {@link #setStatus(ProcessStatus)}.
     * Overrides usually do not need to call this method.
     *
     * @param submitTime The submit time.
     */
    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    /**
     * Sets the start time.
     * Called by {@link #handleStatusChanged(WorkflowStatusEvent)} whose call is triggered by {@link #setStatus(ProcessStatus)}.
     * Overrides usually do not need to call this method.
     *
     * @param startTime The start time.
     */
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    /**
     * Sets the stop time.
     * Called by {@link #handleStatusChanged(WorkflowStatusEvent)} whose call is triggered by {@link #setStatus(ProcessStatus)}.
     * Overrides usually do not need to call this method.
     *
     * @param stopTime The stop time.
     */
    public void setStopTime(Date stopTime) {
        this.stopTime = stopTime;
    }

    /**
     * Returns an array of all registered {@link WorkflowStatusListener}s.
     *
     * @return All listeners.
     */
    public WorkflowStatusListener[] getWorkflowStatusListeners() {
        return statusListeners.toArray(new WorkflowStatusListener[statusListeners.size()]);
    }
}
