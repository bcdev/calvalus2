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

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import org.junit.Ignore;

/**
 * A workflow item that runs through the regular life cycle steps UNKNOWN, SCHEDULED, RUNNING and finally COMPLETED.
 *
 * @author MarcoZ
 * @author Norman
 */
@Ignore
public class LifeStepWorkflowItem extends AbstractWorkflowItem {
    private boolean submitted;

    public boolean isSubmitted() {
        return submitted;
    }

    void incLifeStep() {
        if (isSubmitted()) {
            ProcessStatus status = getStatus();
            if (status.equals(ProcessStatus.UNKNOWN)) {
                setStatus(new ProcessStatus(ProcessState.SCHEDULED));
            } else if (status.equals(ProcessStatus.SCHEDULED)) {
                setStatus(new ProcessStatus(ProcessState.RUNNING, 0.5f));
            } else if (status.equals(new ProcessStatus(ProcessState.RUNNING, 0.5f))) {
                setStatus(new ProcessStatus(ProcessState.COMPLETED));
            }
            // Other case are failures: CANCELLED, ERROR
        }
    }

    @Override
    public void submit() {
        submitted = true;
    }

    @Override
    public void kill() throws WorkflowException {
    }

    @Override
    public void updateStatus() {
    }

    @Override
    public Object[] getJobIds() {
        return new Object[0];
    }
}
