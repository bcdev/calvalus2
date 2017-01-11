package com.bc.calvalus.commons;

/**
 * State of a production.
 *
 * @author Norman
 */
public enum ProcessState {
    /**
     * Indicates an unknown state, e.g. could not be retrieved.
     */
    UNKNOWN(false),
    /**
     * Indicates that the work unit has not yet started.
     */
    SCHEDULED(false),
    /**
     * Indicates that the work unit is in progress.
     */
    RUNNING(false),
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

    /**
     * Returns whether the process is known to be "done".
     *
     * @return {@code true}, if and only if the state is one of {@link ProcessState#COMPLETED}, {@link ProcessState#CANCELLED} or
     *         {@link ProcessState#CANCELLED}. In all other cases {@code false}.
     */
    public boolean isDone() {
        return done;
    }
}
