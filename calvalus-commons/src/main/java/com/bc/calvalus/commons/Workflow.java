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
import java.util.Arrays;
import java.util.List;

/**
 * A collection of one or more work items.
 *
 * @author MarcoZ
 * @author Norman
 */
public abstract class Workflow extends AbstractWorkflowItem implements WorkflowItem.StateChangeListener {
    protected final List<WorkflowItem> itemList;

    protected Workflow(WorkflowItem... items) {
        super();
        this.itemList = new ArrayList<WorkflowItem>();
        add(items);
    }

    public void add(WorkflowItem... items) {
        for (WorkflowItem item : items) {
            item.addStateChangeListener(this);
        }
        itemList.addAll(Arrays.asList(items));
    }

    @Override
    public void kill() throws WorkflowException {
        for (WorkflowItem item : itemList) {
            if (!item.getStatus().isDone()) {
                item.kill();
            }
        }
    }

    @Override
    public void updateStatus() {
        for (WorkflowItem item : itemList) {
            item.updateStatus();
        }
    }

    /**
     * Overridden to aggregate status of contained workflow items.
     * Clients must call {@code super.handleStateChanged(item)}.
     *
     * @param item The item whose state changed.
     */
    @Override
    public void handleStateChanged(WorkflowItem item) {
        ProcessStatus[] statuses = new ProcessStatus[itemList.size()];
        for (int i = 0; i < statuses.length; i++) {
            statuses[i] = itemList.get(i).getStatus();
        }
        setStatus(ProcessStatus.aggregate(statuses));
    }

    @Override
    @Deprecated
    public Object[] getJobIds() {
        ArrayList<Object> list = new ArrayList<Object>();
        for (WorkflowItem item : itemList) {
            list.addAll(Arrays.asList(item.getJobIds()));
        }
        return list.toArray(new Object[list.size()]);
    }

    /**
     * A sequential workflow. An item is submitted only after its predecessor has completed.
     */
    public static class Sequential extends Workflow {
        private int currentItemIndex;

        public Sequential(WorkflowItem... items) {
            super(items);
            currentItemIndex = -1;
        }

        /**
         * Submits the first item. Subsequent items are submitted after their predecessors have successfully completed.
         */
        @Override
        public void submit() throws WorkflowException {
            submitNext();
        }

        @Override
        public void handleStateChanged(WorkflowItem item) {
            super.handleStateChanged(item);
            if (currentItemIndex >= 0
                    && itemList.get(currentItemIndex) == item
                    && itemList.get(currentItemIndex).getStatus().getState() == ProcessState.COMPLETED) {
                submitNext();
            }
        }

        private void submitNext() {
            if (currentItemIndex < itemList.size() - 1) {
                currentItemIndex++;
                try {
                    itemList.get(currentItemIndex).submit();
                } catch (WorkflowException e) {
                    itemList.get(currentItemIndex).setStatus(new ProcessStatus(ProcessState.ERROR, 0.0F, e.getMessage()));
                }
            }
        }
    }

    /**
     * A parallel workflow. All items are submitted independently of each other.
     */
    public static class Parallel extends Workflow {

        public Parallel(WorkflowItem... items) {
            super(items);
        }

        /**
         * Submits all contained items at once.
         */
        @Override
        public void submit() throws WorkflowException {
            for (WorkflowItem item : itemList) {
                item.submit();
            }
        }
    }


}
