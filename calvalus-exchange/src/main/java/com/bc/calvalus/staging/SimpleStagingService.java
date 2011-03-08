package com.bc.calvalus.staging;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A (trivial) staging service implementation.
 *
 * @author MarcoZ
 * @author Norman
 */
public class SimpleStagingService implements StagingService {
    private final ExecutorService executorService;

    public SimpleStagingService(int numParallelThreads) {
        this(Executors.newFixedThreadPool(numParallelThreads));
    }

    public SimpleStagingService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    @Override
    public void orderStaging(Staging staging) throws IOException {
        executorService.submit(staging);
    }
}
