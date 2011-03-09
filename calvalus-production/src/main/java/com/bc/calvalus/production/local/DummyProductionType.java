package com.bc.calvalus.production.local;

import com.bc.calvalus.commons.ProcessState;
import com.bc.calvalus.commons.ProcessStatus;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A production type that does nothing.
 */
class DummyProductionType implements ProductionType {
    private final LocalProcessingService processingService;
    private final StagingService stagingService;

    public DummyProductionType(LocalProcessingService processingService, StagingService stagingService) {
        this.processingService = processingService;
        this.stagingService = stagingService;
    }

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        String name = productionRequest.getProductionParameter("name");
        if (name == null) {
            name = "Doing something";
        }
        String user = productionRequest.getProductionParameter("user");
        if (user == null) {
            user = "someone";
        }

        String jobId = processingService.submitJob();

        String productionId = Production.createId(productionRequest.getProductionType());

        return new Production(productionId,
                              name,
                              user,
                              productionId,
                              productionRequest,
                              jobId);
    }

    @Override
    public Staging createStaging(final Production production) throws ProductionException {
        if (production.getStagingPath() == null) {
            return null;
        }
        return new Staging() {
            @Override
            public String call() throws Exception {
                try {
                    final File outputFile = new File(stagingService.getStagingAreaPath(), production.getStagingPath());
                    production.setStagingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, 0.0f));
                    if (!outputFile.exists()) {
                        File parentFile = outputFile.getParentFile();
                        if (parentFile != null) {
                            parentFile.mkdirs();
                        }
                        FileOutputStream stream = new FileOutputStream(outputFile);
                        byte[] buffer = new byte[1024 * 1024];
                        try {
                            for (int i = 0; i < 32; i++) {
                                if (isCancelled()) {
                                    return null;
                                }
                                production.setStagingStatus(new ProcessStatus(ProcessState.IN_PROGRESS, i / 32f));
                                Arrays.fill(buffer, (byte) i);
                                stream.write(buffer);
                            }
                        } finally {
                            stream.close();
                        }
                    }
                    production.setStagingStatus(new ProcessStatus(ProcessState.COMPLETED, 1f));
                } catch (IOException e) {
                    production.setStagingStatus(new ProcessStatus(ProcessState.ERROR, production.getStagingStatus().getProgress(), e.getMessage()));
                }
                return production.getStagingPath();
            }

            @Override
            public void cancel() {
                super.cancel();
                production.setStagingStatus(new ProcessStatus(ProcessState.CANCELLED));
            }
        };
    }

    @Override
    public boolean accepts(ProductionRequest productionRequest) {
        return true;
    }
}
