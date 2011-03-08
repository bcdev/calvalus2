package com.bc.calvalus.production;

import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestStagingService implements StagingService {
    private List<Staging> stagings = new ArrayList<Staging>();

    public List<Staging> getStagings() {
        return stagings;
    }

    @Override
    public void orderStaging(Staging staging) throws IOException {
        stagings.add(staging);
    }
}
