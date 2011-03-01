package com.bc.calvalus.production;

/**
* Created by IntelliJ IDEA.
* User: Norman
* Date: 01.03.11
* Time: 10:54
* To change this template use File | Settings | File Templates.
*/
public enum ProductionState {
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
