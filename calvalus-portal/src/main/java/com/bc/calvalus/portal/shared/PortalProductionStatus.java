package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class PortalProductionStatus implements IsSerializable {
    private static final float EPS = 1.0E-04f;
    private PortalProductionState state;
    private String message;
    private float progress;

    /**
     * No-arg constructor as required by {@link IsSerializable}.
     */
    public PortalProductionStatus() {
        this(PortalProductionState.WAITING);
    }

    public PortalProductionStatus(PortalProductionState state) {
        this(state, 0.0f);
    }

    public PortalProductionStatus(PortalProductionState state, float progress) {
        this(state, "", progress);
    }

    public PortalProductionStatus(PortalProductionState state, String message, float progress) {
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

    public PortalProductionState getState() {
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
