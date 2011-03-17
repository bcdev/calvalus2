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

import java.util.EventObject;

/**
* A workflow status change event.
*/
public class WorkflowStatusEvent extends EventObject {
    private final ProcessStatus oldStatus;
    private final ProcessStatus newStatus;

    public WorkflowStatusEvent(WorkflowItem source, ProcessStatus oldStatus, ProcessStatus newStatus) {
        super(source);
        this.oldStatus = oldStatus;
        this.newStatus = newStatus;
    }

    @Override
    public WorkflowItem getSource() {
        return (WorkflowItem) super.getSource();
    }

    public ProcessStatus getOldStatus() {
        return oldStatus;
    }

    public ProcessStatus getNewStatus() {
        return newStatus;
    }
}
