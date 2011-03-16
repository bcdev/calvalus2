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
 * Clients only must implement {@link #submit()}.
 *
 * @author Norman
 */
public abstract class AbstractWorkflowItem implements WorkflowItem {
    private final List<StateChangeListener> changeListeners;
    private ProcessStatus status;

    public AbstractWorkflowItem() {
        this.changeListeners = new ArrayList<StateChangeListener>();
        this.status = ProcessStatus.UNKNOWN;
    }

    @Override
    public void addStateChangeListener(StateChangeListener l) {
        changeListeners.add(l);
    }

    @Override
    public ProcessStatus getStatus() {
        return status;
    }

    @Override
    public void setStatus(ProcessStatus status) {
        ProcessState oldState = this.status.getState();
        this.status = status;
        if (oldState != status.getState()) {
            // todo: do not allow transition to unknown state
            fireStateChanged();
        }
    }

    protected void fireStateChanged() {
        for (StateChangeListener changeListener : changeListeners) {
            changeListener.handleStateChanged(this);
        }
    }
}
