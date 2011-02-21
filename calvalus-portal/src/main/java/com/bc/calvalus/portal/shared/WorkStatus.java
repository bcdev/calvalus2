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
        /**
         * Indicates that the work unit has not yet started.
         */
        WAITING,
        /**
         * Indicates that the work unit is in progress.
         */
        IN_PROGRESS,
        /**
         * Indicates that the work unit has been successfully completed.
         */
        COMPLETED,
        /**
         * Indicates that the work unit has been cancelled, e.g. on user request.
         */
        CANCELLED,
        /**
         * Indicates an unknown state, e.g. could not be retrieved.
         */
        UNKNOWN,
        /**
         * Indicates a server-side problem, e.g. missing input files.
         */
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
        return !(state == State.WAITING || state == State.IN_PROGRESS);
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
