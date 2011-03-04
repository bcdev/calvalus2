package com.bc.calvalus.production.hadoop;

import java.util.concurrent.Callable;

public abstract class StagingJob implements Callable<StagingJob> {
    @Override
    public abstract StagingJob call() throws Exception;

    public abstract boolean isCancelled();

    public abstract void cancel();
}
