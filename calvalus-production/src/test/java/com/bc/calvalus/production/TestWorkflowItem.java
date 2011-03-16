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

package com.bc.calvalus.production;

import com.bc.calvalus.commons.AbstractWorkflowItem;
import com.bc.calvalus.commons.WorkflowException;
import org.junit.Ignore;

/**
 * A simple WorkflowItem with an ID but that does nothing on submit(), kill() and updateStatus().
 */
@Ignore
public class TestWorkflowItem<T> extends AbstractWorkflowItem {
    private final T jobId;

    TestWorkflowItem(T jobId) {
        this.jobId = jobId;
    }

    public T getJobId() {
        return jobId;
    }

    @Override
    public void submit() throws WorkflowException {
    }

    @Override
    public void kill() throws WorkflowException {
    }

    @Override
    public void updateStatus() {
    }

    @Override
    public Object[] getJobIds() {
        return new Object[]{getJobId()};
    }
}
