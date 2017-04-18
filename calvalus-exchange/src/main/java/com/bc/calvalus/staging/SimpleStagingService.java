package com.bc.calvalus.staging;

import org.esa.snap.core.util.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Observable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A (trivial) staging service implementation.
 *
 * @author MarcoZ
 * @author Norman
 */
public class SimpleStagingService implements StagingService {
    private final File stagingDir;
    private final ExecutorService executorService;
    private Observable productionService;

    public SimpleStagingService(File stagingDir, int numParallelJobs) throws IOException {
        this.stagingDir = stagingDir.getCanonicalFile();
        this.executorService = Executors.newFixedThreadPool(numParallelJobs);
    }

    @Override
    public File getStagingDir() {
        return stagingDir;
    }

    @Override
    public void submitStaging(Staging staging) throws IOException {
        executorService.execute(staging);
    }

    @Override
    public void deleteTree(String path) throws IOException {
        File treeToDelete = new File(stagingDir, path).getCanonicalFile();
        String stagingDirPath = stagingDir.getPath();
        String treeToDeletePath = treeToDelete.getPath();
        if (treeToDeletePath.startsWith(stagingDirPath)
                && treeToDeletePath.length() > stagingDirPath.length()) {
            FileUtils.deleteTree(treeToDelete);
        } else {
           throw new IOException("Illegal path: " + treeToDeletePath);
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
    }

    @Override
    public void setProductionService(Observable productionService)
    {
        this.productionService = productionService;
    }

    @Override
    public Observable getProductionService() {
        return productionService;
    }
}
