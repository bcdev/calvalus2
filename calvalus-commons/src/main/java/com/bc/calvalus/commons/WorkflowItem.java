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

/**
 * A workflow item. Clients should not implement this interface directly. Instead, they should
 * use {@link AbstractWorkflowItem} as base class.
 *
 * @author MarcoZ
 * @author Norman
 */
public interface WorkflowItem {
    /**
     * Submits this work item's job to the underlying job engine so that a new process is spawn.
     * Usually, implementations of this method should return immediately after the job has been submitted.
     * In rare cases the method blocks, e.g. until the associated process has terminated.
     *
     * @throws WorkflowException If the submission fails.
     */
    void submit() throws WorkflowException;

    /**
     * Kills this work item's process in the underlying job engine.
     * Usually, implementations of this method should return immediately after the kill request has been submitted.
     * In rare cases the method blocks, e.g. until the job has been terminated.
     *
     * @throws WorkflowException If the kill request fails.
     */
    void kill() throws WorkflowException;

    /**
     * Asks this workflow item to update its process status from the underlying job engine.
     */
    void updateStatus();

    /**
     * @return The current workflow item's process status.
     */
    ProcessStatus getStatus();

    /**
     * @param status The new workflow item's process status.
     */
    void setStatus(ProcessStatus status);

    /**
     * Adds a new workflow status change listener to the workflow.
     * The listener is called each time the status of this workflow item's
     * process changes.
     *
     * @param listener The new workflow status change listener.
     */
    void addWorkflowStatusListener(WorkflowStatusListener listener);

    /**
     * Gets the identifiers of all the jobs that this workflow is managing.
     * The type of the job identifiers depends on the underlying job engine(s) used.
     *
     * @return The array of job identifiers.
     */
    @Deprecated
    Object[] getJobIds();

}
