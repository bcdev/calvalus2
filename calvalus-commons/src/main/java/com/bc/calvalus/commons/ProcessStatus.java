package com.bc.calvalus.commons;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class ProcessStatus {
    public final static ProcessStatus UNKNOWN = new ProcessStatus(ProcessState.UNKNOWN);
    public final static ProcessStatus SCHEDULED = new ProcessStatus(ProcessState.SCHEDULED);

    private static final float EPS = 1.0E-04f;
    private final ProcessState state;
    private final float progress;
    private final String message;

    public ProcessStatus(ProcessState state) {
        this(state, state.isDone() ? 1.0f : 0.0f);
    }

    public ProcessStatus(ProcessState state, float progress) {
        this(state, progress, "");
    }

    public ProcessStatus(ProcessState state, float progress, String message) {
        if (state == null) {
            throw new NullPointerException("state");
        }
        if (message == null) {
            throw new NullPointerException("message");
        }
        this.state = state;
        this.message = message;
        this.progress = progress;
    }

    /**
     * Aggregates the given statuses.
     * <ul>
     * <li>The resulting <b>state</b> is computed as follows:
     * <ol>
     * <li>If a single state is CANCELLED: CANCELLED</li>
     * <li>If a single state is ERROR: ERROR</li>
     * <li>If all states are COMPLETE: COMPLETE</li>
     * <li>If some are COMPLETE or some are RUNNING: RUNNING</li>
     * <li>If none are COMPLETE or RUNNING, but some are SCHEDULED: SCHEDULED</li>
     * <li>If neither of the above: UNKNOWN</li>
     * </ol>
     * </li>
     * <li>The resulting <b>progress</b> of is the average progress of all statuses</b></li>
     * <li>The resulting <b>message</b> is either the CANCELLED / ERROR message or
     * the first non-empty message</b></li>
     * </ul>
     *
     * @param statuses The statuses to aggregate. If {@code statuses} is not given,
     * the method returns {@link ProcessStatus#UNKNOWN} in order to avoid returning {@code null}.
     * @return The aggregated status.
     * @throws NullPointerException if any of the given {@code statuses} is {@code null}.
     */
    public static ProcessStatus aggregate(ProcessStatus... statuses) {
        assertStatusesNotNull(statuses);

        if (statuses.length == 0) {
            return UNKNOWN;
        } else if (statuses.length == 1) {
            return statuses[0];
        }

        float averageProgress = getAverageProgress(statuses);

        for (ProcessStatus status : statuses) {
            if (status.getState() == ProcessState.CANCELLED) {
                return new ProcessStatus(status.getState(), averageProgress, status.getMessage());
            }
        }

        for (ProcessStatus status : statuses) {
            if (status.getState() == ProcessState.ERROR) {
                return new ProcessStatus(status.getState(), averageProgress, status.getMessage());
            }
        }

        String message = "";
        for (ProcessStatus status : statuses) {
            message = status.getMessage();
            if (!message.isEmpty()) {
                break;
            }
        }

        final ProcessState state = getCummulativeState(statuses);

        return new ProcessStatus(state, averageProgress, message);
    }

    private static void assertStatusesNotNull(ProcessStatus[] statuses) {
        for (int i = 0; i < statuses.length; i++) {
            if (statuses[i] == null) {
                throw new NullPointerException("statuses[" + i + "]");
            }
        }
    }

    private static float getAverageProgress(ProcessStatus[] statuses) {
        float averageProgress = 0f;
        for (ProcessStatus status : statuses) {
            averageProgress += status.getProgress();
        }
        return averageProgress / statuses.length;
    }

    private static ProcessState getCummulativeState(ProcessStatus[] statuses) {
        Map<ProcessState, Integer> stateCounters = new EnumMap<ProcessState, Integer>(ProcessState.class);
        // Init state counts to zero
        for (ProcessState value : ProcessState.values()) {
            stateCounters.put(value, 0);
        }
        // Count states
        for (ProcessStatus status : statuses) {
            int oldCount = stateCounters.get(status.getState());
            stateCounters.put(status.getState(), oldCount + 1);
        }
        // All are COMPLETE --> COMPLETE
        if (stateCounters.get(ProcessState.COMPLETED) == statuses.length) {
            return ProcessState.COMPLETED;
        }
        // Only some are COMPLETE --> So its must be still RUNNING
        if (stateCounters.get(ProcessState.COMPLETED) > 0) {
            return ProcessState.RUNNING;
        }
        // None COMPLETE, some are RUNNING --> So its RUNNING
        if (stateCounters.get(ProcessState.RUNNING) > 0) {
            return ProcessState.RUNNING;
        }
        // None COMPLETE, nore RUNNING, some are SCHEDULED --> So its SCHEDULED
        if (stateCounters.get(ProcessState.SCHEDULED) > 0) {
            return ProcessState.SCHEDULED;
        }
        // UNKNOWN if the state is none of the above.
        return ProcessState.UNKNOWN;
    }

    public ProcessState getState() {
        return state;
    }

    public boolean isDone() {
        return state.isDone();
    }

    public String getMessage() {
        return message;
    }

    public float getProgress() {
        return progress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcessStatus that = (ProcessStatus) o;

        float delta = that.progress - progress;
        if (delta < 0) {
            delta = -delta;
        }

        return delta <= EPS
                && message.equals(that.message)
                && state == that.state;
    }

    @Override
    public int hashCode() {
        int result = state.hashCode();
        result = 31 * result + message.hashCode();
        result = 31 * result + (int) (progress / EPS);
        return result;
    }

    @Override
    public String toString() {
        return "ProductionStatus{" +
                "state=" + state +
                ", message='" + message + '\'' +
                ", progress=" + progress +
                '}';
    }

    public static ProcessStatus aggregateUnsustainable(ProcessStatus... statuses) {
        assertStatusesNotNull(statuses);
        List<ProcessStatus> usable = new ArrayList<ProcessStatus>(statuses.length);
        for (ProcessStatus status : statuses) {
            if (status.getState() !=  ProcessState.ERROR && status.getState() !=  ProcessState.CANCELLED) {
                usable.add(status);
            }
        }
        return aggregate(usable.toArray(new ProcessStatus[usable.size()]));
    }
}
