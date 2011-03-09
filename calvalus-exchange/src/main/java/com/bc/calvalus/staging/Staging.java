package com.bc.calvalus.staging;

import java.util.concurrent.Callable;

public abstract class Staging implements Callable<String> {
    private boolean cancelled;

    /**
     * Performs the staging.
     *
     * @return The relative staging path.
     * @throws Exception if an error occurs.
     */
    @Override
    public abstract String call() throws Exception;

    public final boolean isCancelled() {
        return cancelled;
    }

    public void cancel() {
        cancelled = true;
    }
}
