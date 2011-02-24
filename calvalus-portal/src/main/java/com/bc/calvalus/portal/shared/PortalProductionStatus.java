package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class PortalProductionStatus implements IsSerializable {
    private static final float EPS = 1.0E-04f;
    private State state;
    private String message;
    private float progress;

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
    public PortalProductionStatus() {
        this(State.WAITING);
    }

    public PortalProductionStatus(State state) {
        this(state, "", 0.0f);
    }

    public PortalProductionStatus(State state, String message, float progress) {
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

        PortalProductionStatus that = (PortalProductionStatus) o;

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
        return "PortalProductionStatus{" +
                "state=" + state +
                ", message='" + message + '\'' +
                ", progress=" + progress +
                '}';
    }
}
