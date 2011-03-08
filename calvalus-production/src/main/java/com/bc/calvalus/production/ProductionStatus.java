package com.bc.calvalus.production;

/**
 * Provides status information about a server-side production transaction.
 *
 * @author Norman
 */
public class ProductionStatus {
    public final static ProductionStatus UNKNOWN = new ProductionStatus(ProductionState.UNKNOWN);
    public final static ProductionStatus WAITING = new ProductionStatus(ProductionState.WAITING);

    private static final float EPS = 1.0E-04f;
    private final ProductionState state;
    private final float progress;
    private final String message;

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

    public static ProductionStatus aggregate(ProductionStatus... statuses) {
        if (statuses.length == 0) {
            return null;
        }
        if (statuses.length == 1) {
            return statuses[0];
        }

        float averageProgress = 0f;
        for (ProductionStatus jobStatus : statuses) {
            averageProgress += jobStatus.getProgress();
        }
        averageProgress /= statuses.length;

        for (ProductionStatus status : statuses) {
            if (status.getState() == ProductionState.ERROR
                    || status.getState() == ProductionState.CANCELLED) {
                return new ProductionStatus(status.getState(), averageProgress, status.getMessage());
            }
        }

        String message = "";
        for (ProductionStatus status : statuses) {
            message = status.getMessage();
            if (!message.isEmpty()) {
                break;
            }
        }

        int numCompleted = 0;
        int numWaiting = 0;
        int numUnknown = 0;
        for (ProductionStatus status : statuses) {
            if (status.getState() == ProductionState.COMPLETED) {
                numCompleted++;
            } else if (status.getState() == ProductionState.WAITING) {
                numWaiting++;
            } else if (status.getState() == ProductionState.UNKNOWN) {
                numUnknown++;
            }
        }

        final ProductionState state;
        if (numCompleted == statuses.length) {
            state = ProductionState.COMPLETED;
        } else if (numWaiting == statuses.length) {
            state = ProductionState.WAITING;
        } else if (numUnknown == statuses.length) {
            state = ProductionState.UNKNOWN;
        } else {
            state = ProductionState.IN_PROGRESS;
        }

        return new ProductionStatus(state, averageProgress, message);
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
