package com.bc.calvalus.production;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class ProductionStatus {
    private static final float EPS = 1.0E-04f;
    private ProductionState state;
    private float progress;
    private String message;

    public ProductionStatus() {
        this(ProductionState.UNKNOWN);
    }

    public ProductionStatus(ProductionState state) {
        this(state, state.isDone() ? 1.0f : 0.0f);
    }

    public ProductionStatus(ProductionState state, float progress) {
        this(state, progress, "");
    }

    public ProductionStatus(ProductionState state, float progress, String message) {
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

    public ProductionState getState() {
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

        ProductionStatus that = (ProductionStatus) o;

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
