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
import java.util.List;

/**
 * Base class for workflow item implementations.
 *
 * @author Norman
 */
public abstract class AbstractWorkflowItem implements WorkflowItem {
    private static final WorkflowItem[] NO_ITEMS = new WorkflowItem[0];
    private final List<WorkflowStatusListener> statusListeners;
    private ProcessStatus status;

    public AbstractWorkflowItem() {
        this.statusListeners = new ArrayList<WorkflowStatusListener>();
        this.status = ProcessStatus.UNKNOWN;
    }

    @Override
    public void addWorkflowStatusListener(WorkflowStatusListener l) {
        statusListeners.add(l);
    }

    @Override
    public ProcessStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ProcessStatus newStatus) {
        if (!status.equals(newStatus)) {
            // todo: do not allow transition to unknown state
            ProcessStatus oldStatus = status;
            status = newStatus;
            fireStatusChanged(new WorkflowStatusEvent(this, oldStatus, newStatus));
        }
    }

    @Override
    public WorkflowItem[] getItems() {
        return NO_ITEMS;
    }

    protected void fireStatusChanged(WorkflowStatusEvent event) {
        for (WorkflowStatusListener changeListener : statusListeners) {
            changeListener.handleStatusChanged(event);
        }
    }
}
