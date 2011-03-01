package com.bc.calvalus.production;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class ProductionStatus {
    private static final float EPS = 1.0E-04f;
    private ProductionState state;
    private String message;
    private float progress;

    public ProductionStatus() {
        this(ProductionState.UNKNOWN);
    }

    public ProductionStatus(ProductionState state) {
        this(state, 0.0f);
    }

    public ProductionStatus(ProductionState state, float progress) {
        this(state, null, progress);
    }

    public ProductionStatus(ProductionState state, String message, float progress) {
        if (state == null) {
            throw new NullPointerException("state");
        }
        this.state = state;
        this.message = message;
        this.progress = progress;
    }

    public ProductionState getState() {
        return state;
    }

    public boolean isDone() {
        return state == ProductionState.COMPLETED
                || state == ProductionState.ERROR
                || state == ProductionState.CANCELLED;
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
                && (message != null ? message.equals(that.message) : that.message == null)
                && state == that.state;
    }

    @Override
    public int hashCode() {
        int result = state.hashCode();
        result = 31 * result + (message != null ? message.hashCode() : 0);
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
