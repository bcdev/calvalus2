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
    private final String stagingAreaPath;
    private final ExecutorService executorService;

    public SimpleStagingService(String stagingAreaPath, int numParallelThreads) {
        this.stagingAreaPath = stagingAreaPath;
        this.executorService = Executors.newFixedThreadPool(numParallelThreads);
    }

    @Override
    public String getStagingAreaPath() {
        return stagingAreaPath;
    }

    @Override
    public void submitStaging(Staging staging) throws IOException {
        executorService.submit(staging);
    }
}
