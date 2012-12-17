package com.bc.calvalus.portal.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

/**
 * GWT-serializable version of the {@link com.bc.calvalus.commons.ProcessState} class.
 *
 * @author Norman
 */
public enum DtoProcessState implements IsSerializable {
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

    DtoProcessState(boolean done) {
        this.done = done;
    }

    public boolean isDone() {
        return done;
    }
}
