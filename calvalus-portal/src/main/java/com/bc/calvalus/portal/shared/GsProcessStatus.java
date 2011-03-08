package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.production.ProcessStatus} class.
 *
 * @author Norman
 */
public class GsProcessStatus implements IsSerializable {
    private static final float EPS = 1.0E-04f;
    private GsProcessState state;
    private String message;
    private float progress;

    /**
     * No-arg constructor as required by {@link IsSerializable}.
     */
    public GsProcessStatus() {
        this(GsProcessState.UNKNOWN);
    }

    public GsProcessStatus(GsProcessState state) {
        this(state, 0.0f);
    }

    public GsProcessStatus(GsProcessState state, float progress) {
        this(state, "", progress);
    }

    public GsProcessStatus(GsProcessState state, String message, float progress) {
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

    public GsProcessState getState() {
        return state;
    }

    public boolean isDone() {
        return state.isDone();
    }

    public boolean isUnknown() {
        return state == GsProcessState.UNKNOWN;
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

        GsProcessStatus that = (GsProcessStatus) o;

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
