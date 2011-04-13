package com.bc.calvalus.commons;

import java.util.EnumMap;
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

    public static ProcessStatus aggregate(ProcessStatus... statuses) {
        for (int i = 0; i < statuses.length; i++) {
            if (statuses[i] == null) {
                throw new NullPointerException("statuses[" + i + "]");
            }
        }

        if (statuses.length == 0) {
            return null;
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

    private static float getAverageProgress(ProcessStatus[] statuses) {
        float averageProgress = 0f;
        for (ProcessStatus status : statuses) {
            averageProgress += status.getProgress();
        }
        return averageProgress / statuses.length;
    }

    private static ProcessState getCummulativeState(ProcessStatus[] statuses) {
        Map<ProcessState, Integer> map = new EnumMap<ProcessState, Integer>(ProcessState.class);
        for (ProcessState value : ProcessState.values()) {
            map.put(value, 0);
        }
        for (ProcessStatus status : statuses) {
            map.put(status.getState(), map.get(status.getState()) + 1);
        }
        if (map.get(ProcessState.COMPLETED) == statuses.length) {
            return ProcessState.COMPLETED;
        }
        if (map.get(ProcessState.COMPLETED) > 0) {
            return ProcessState.RUNNING;
        }
        if (map.get(ProcessState.RUNNING) > 0) {
            return ProcessState.RUNNING;
        }
        if (map.get(ProcessState.SCHEDULED) > 0) {
            return ProcessState.SCHEDULED;
        }
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
}
