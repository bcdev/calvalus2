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

/**
 * A workflow item that runs through the regular life cycle steps UNKNOWN, SCHEDULED, RUNNING and finally COMPLETED.
 *
 * @author Norman
 */
@Ignore
public class FailingLifeStepWorkflowItem extends LifeStepWorkflowItem {
    private final ProcessState stateBeforeFailure;
    private final ProcessState failureState;

    public FailingLifeStepWorkflowItem(ProcessState stateBeforeFailure, ProcessState failureState) {
        this.stateBeforeFailure = stateBeforeFailure;
        this.failureState = failureState;
    }

    @Override
    void incLifeStep() {
        if (isSubmitted()) {
            ProcessStatus status = getStatus();
            if (status.getState() == stateBeforeFailure) {
                setStatus(new ProcessStatus(failureState));
            }else {
                super.incLifeStep();
            }
        }
    }

}
