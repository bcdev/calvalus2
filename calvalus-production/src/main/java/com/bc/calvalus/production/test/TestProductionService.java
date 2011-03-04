package com.bc.calvalus.production.test;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.production.ProductionProcessor;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An ProductionService implementation that is useful for developing the portal.
 * To use it, specify the servlet init-parameter 'calvalus.portal.backendService.class'
 * (context.xml or web.xml)
 */
public class TestProductionService implements ProductionService {

    private final List<TestProduction> productionList;
    private final String relStagingUrl;
    private final File localStagingDir;
    private long counter;

    public TestProductionService(String relStagingUrl, File localStagingDir) {
        this.relStagingUrl = relStagingUrl;
        this.localStagingDir = localStagingDir;
        productionList = Collections.synchronizedList(new ArrayList<TestProduction>(32));
        // Add some dummy productions
        productionList.add(new TestProduction("Formatting all hard drives", 20 * 1000,
                                              relStagingUrl + "/p1", new File(localStagingDir, "p1"), true));
        productionList.add(new TestProduction("Drying CD slots", 10 * 1000,
                                              relStagingUrl + "/p2", new File(localStagingDir, "p2"), true));
        productionList.add(new TestProduction("Rewriting kernel using BASIC", 5 * 1000,
                                              relStagingUrl + "/p3", new File(localStagingDir, "p3"), false));
        for (TestProduction production : productionList) {
            production.start();
        }
    }

    @Override
    public ProductSet[] getProductSets(String filter) throws ProductionException {
        // Return some dummy product sets
        return new ProductSet[]{
                new ProductSet("ps1", "MERIS-L1B", "MERIS RR 2004-2009"),
                new ProductSet("ps2", "MERIS-L1B", "MERIS RR 2004"),
                new ProductSet("ps3", "MERIS-L1B", "MERIS RR 2005"),
                new ProductSet("ps4", "MERIS-L1B", "MERIS RR 2006"),
                new ProductSet("ps5", "MERIS-L1B", "MERIS RR 2007"),
                new ProductSet("ps6", "MERIS-L1B", "MERIS RR 2008"),
                new ProductSet("ps7", "MERIS-L1B", "MERIS RR 2009"),
        };
    }

    @Override
    public ProductionProcessor[] getProcessors(String filter) throws ProductionException {
        // Return some dummy processors
        return new ProductionProcessor[]{
                new ProductionProcessor("pc1", "MERIS IOP Case2R",
                              "",
                              "beam-meris-case2r",
                              new String[]{"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                new ProductionProcessor("pc2", "MERIS IOP QAA",
                              "",
                              "beam-meris-qaa",
                              new String[]{"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
                new ProductionProcessor("pc3", "Band Maths",
                              "",
                              "beam-gpf",
                              new String[]{"4.8"}),
        };
    }

    @Override
    public Production[] getProductions(String filter) throws ProductionException {
        return productionList.toArray(new Production[productionList.size()]);
    }

    @Override
    public ProductionResponse orderProduction(ProductionRequest productionRequest) throws ProductionException {

        String productionType = productionRequest.getProductionType();
        String outputFileName = getOutputFile(productionRequest);
        String inputProductSetId = productionRequest.getProductionParameters().get("inputProductSetId");
        String productionName = MessageFormat.format("Producing file ''{0}'' from ''{1}'' using workflow ''{2}''",
                                                     outputFileName,
                                                     inputProductSetId,
                                                     productionType);

        long secondsToRun = (int) (10 + 20 * Math.random()); // 10...30 seconds

        TestProduction production = new TestProduction(productionName, secondsToRun * 1000,
                                                       relStagingUrl + "/" + outputFileName,
                                                       new File(localStagingDir, outputFileName),
                                                       Boolean.parseBoolean(productionRequest.getProductionParameter("outputStaging")));
        production.start();
        productionList.add(production);
        return new ProductionResponse(production);
    }

    @Override
    public void cancelProductions(String[] productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            TestProduction production = getProduction(productionId);
            if (production != null) {
                production.cancel();
                count++;
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been cancelled.", count, productionIds.length));
        }
    }

    @Override
    public void deleteProductions(String[] productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            TestProduction production = getProduction(productionId);
            if (production != null) {
                production.cancel();
                productionList.remove(production);
                count++;
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been deleted.", count, productionIds.length));
        }
    }

    @Override
    public void stageProductions(String[] productionIds) throws ProductionException {
        int count = 0;
        for (String productionId : productionIds) {
            TestProduction production = getProduction(productionId);
            if (production != null) {
                production.stageOutput();
                count++;
            }
        }
        if (count < productionIds.length) {
            throw new ProductionException(String.format("Only %d of %d production(s) have been staged.", count, productionIds.length));
        }
    }

    private TestProduction getProduction(String productionId) {
        for (TestProduction production : productionList) {
            if (productionId.equals(production.getId())) {
                return production;
            }
        }
        return null;
    }

    private String getOutputFile(ProductionRequest productionRequest) {
        return productionRequest.getProductionParameters().get("outputFileName")
                .replace("${user}", System.getProperty("user.name"))
                .replace("${type}", productionRequest.getProductionType())
                .replace("${num}", (++counter) + "");
    }

}
