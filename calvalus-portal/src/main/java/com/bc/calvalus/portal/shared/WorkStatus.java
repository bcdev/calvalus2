package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class WorkStatus implements IsSerializable {
    private State state;
    private String message;
    private double progress;

    public enum State {
        WAITING,
        IN_PROGRESS,
        DONE,
        CANCELLED,
        ERROR,
    }

    /**
     * No-arg constructor as required by {@link IsSerializable}.
     */
    public WorkStatus() {
        this(State.WAITING, "", 0.0);
    }

    public WorkStatus(State state, String message, double progress) {
        this.state = state;
        this.message = message;
        this.progress = progress;
    }

    public State getState() {
        return state;
    }

    public boolean isDone() {
        return state == State.DONE || state == State.ERROR  || state == State.CANCELLED;
    }

    public String getMessage() {
        return message;
    }

    public double getProgress() {
        return progress;
    }

    @Override
    public String toString() {
        return "WorkStatus{" +
                "state=" + state +
                ", message='" + message + '\'' +
                ", progress=" + progress +
                '}';
    }
}
