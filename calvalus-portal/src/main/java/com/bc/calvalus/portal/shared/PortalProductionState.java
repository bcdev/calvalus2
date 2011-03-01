package com.bc.calvalus.portal.shared;

/**
* State of a production.
*/
public enum PortalProductionState {
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
     * Indicates an unknown state, e.g. could not be retrieved.
     */
    UNKNOWN(false),
    /**
     * Indicates a server-side problem, e.g. missing input files.
     */
    ERROR(true);

    private final boolean done;

    PortalProductionState(boolean done) {
        this.done = done;
    }

    public boolean isDone() {
        return done;
    }
}
