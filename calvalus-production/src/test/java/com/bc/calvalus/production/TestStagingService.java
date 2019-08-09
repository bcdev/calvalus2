package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.junit.Ignore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

@Ignore
public class TestStagingService implements StagingService {
    private List<Staging> stagings = new ArrayList<Staging>();
    private boolean closed;
    private Observable productionService;

    public TestStagingService() {
    }

    public List<Staging> getStagings() {
        return stagings;
    }

    @Override
    public File getStagingDir() {
        return new File("/opt/tomcat/webapps/calvalus/staging");
    }

    @Override
    public void submitStaging(Staging staging) throws IOException {
        stagings.add(staging);
    }

    @Override
    public void deleteTree(String path) throws IOException {
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void setProductionService(Observable productionService) {
        this.productionService = productionService;
    }

    @Override
    public Observable getProductionService() {
        return productionService;
    }

    public boolean isClosed() {
        return closed;
    }
}
