package com.bc.calvalus.production;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;

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
    final List<WorkflowItem> itemList;

    protected Workflow() {
        super();
        this.itemList = new ArrayList<WorkflowItem>();
    }

    public void addItem(WorkflowItem... items) {
        for (WorkflowItem item : items) {
            item.addStateChangeListener(this);
        }
        itemList.addAll(Arrays.asList(items));
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

    /**
     * A sequential workflow. An item is submitted only after its predecessor has completed.
     */
    public static class Sequential extends Workflow {
        int currentIndex = -1;

        /**
         * Submits the first item. Subsequent items are submitted after their predecessors have successfully completed.
         */
        @Override
        public void submit() {
            submitNext();
        }

        @Override
        public void kill() {
            // todo - test & implement me!
        }

        @Override
        public void handleStateChanged(WorkflowItem item) {
            super.handleStateChanged(item);
            if (currentIndex >= 0
                    && itemList.get(currentIndex) == item
                    && itemList.get(currentIndex).getStatus().getState() == ProcessState.COMPLETED) {
                submitNext();
            }
        }

        private void submitNext() {
            if (currentIndex < itemList.size() - 1) {
                currentIndex++;
                itemList.get(currentIndex).submit();
            }
        }
    }

    /**
     * A parallel workflow. All items are submitted independently of each other.
     */
    public static class Parallel extends Workflow {
        /**
         * Submits all contained items at once.
         */
        @Override
        public void submit() {
            for (WorkflowItem item : itemList) {
                item.submit();
            }
        }

        @Override
        public void kill() {
            // todo - test & implement me!
        }
    }


}
