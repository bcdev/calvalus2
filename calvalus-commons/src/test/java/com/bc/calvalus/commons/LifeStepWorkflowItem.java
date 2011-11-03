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

import org.junit.Ignore;

import java.util.Date;

/**
 * A workflow item that runs through the regular life cycle steps UNKNOWN, SCHEDULED, RUNNING and finally COMPLETED.
 *
 * @author MarcoZ
 * @author Norman
 */
@Ignore
public class LifeStepWorkflowItem extends AbstractWorkflowItem {
    private int submitCount;
    private Date time;
    private int lifeSteps;

    public LifeStepWorkflowItem() {
        this(1);
    }

    public LifeStepWorkflowItem(int lifeSteps) {
        this.lifeSteps = lifeSteps;
    }

    public boolean isSubmitted() {
        return submitCount > 0;
    }

    public int getSubmitCount() {
        return submitCount;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    void incLifeStep() {
        if (isSubmitted()) {
            ProcessStatus status = getStatus();
            if (status.equals(ProcessStatus.UNKNOWN)) {
                setStatus(new ProcessStatus(ProcessState.SCHEDULED));
            } else if (status.equals(ProcessStatus.SCHEDULED)) {
                setStatus(new ProcessStatus(ProcessState.RUNNING, 0.5f));
                lifeSteps--;
            } else if (status.getState().equals(ProcessState.RUNNING) && lifeSteps > 0) {
                lifeSteps--;
            } else if (status.getState().equals(ProcessState.RUNNING)) {
                setStatus(new ProcessStatus(ProcessState.COMPLETED));
            } else if (status.equals(new ProcessStatus(ProcessState.COMPLETED))) {
                setStatus(new ProcessStatus(ProcessState.COMPLETED, 1.0F, "Completed, but message changed. Submit count: " + getSubmitCount()));
            }
            // Other case are failures: CANCELLED, ERROR
        }
    }

    @Override
    public void submit() {
        submitCount++;
    }

    @Override
    public void kill() throws WorkflowException {
        setStatus(new ProcessStatus(ProcessState.ERROR));
    }

    @Override
    public void updateStatus() {
    }

    @Override
    protected void fireStatusChanged(WorkflowStatusEvent event) {
        super.fireStatusChanged(new WorkflowStatusEvent(event.getSource(),
                                                        event.getOldStatus(),
                                                        event.getNewStatus(),
                                                        time));
    }
}
