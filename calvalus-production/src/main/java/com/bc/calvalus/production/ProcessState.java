package com.bc.calvalus.production;

/**
 * State of a production.
 */
public enum ProcessState {
    /**
     * Indicates an unknown state, e.g. could not be retrieved.
     */
    UNKNOWN(false),
    /**
     * Indicates that the work unit has not yet started.
     */
    WAITING(false),
    /**
     * Indicates that the work unit is in progress.
     */
    IN_PROGRESS(false),
    /**
     * Indicates that the work unit has been successfully completed.
     */
    COMPLETED(true),
    /**
     * Indicates that the work unit has been cancelled, e.g. on user request.
     */
    CANCELLED(true),
    /**
     * Indicates a server-side problem, e.g. missing input files.
     */
    ERROR(true);

    private final boolean done;

    ProcessState(boolean done) {
        this.done = done;
    }

    public boolean isDone() {
        return done;
    }
}
