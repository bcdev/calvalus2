package com.bc.calvalus.production.test;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.production.Processor;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionParameter;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionResponse;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionState;
import com.bc.calvalus.production.ProductionStatus;

import javax.servlet.ServletContext;
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
    private long counter;

    public TestProductionService() {
        productionList = Collections.synchronizedList(new ArrayList<TestProduction>(32));
        // Add some dummy productions
        productionList.add(new TestProduction("Formatting all hard drives", 20 * 1000, null));
        productionList.add(new TestProduction("Drying CD slots", 10 * 1000, null));
        productionList.add(new TestProduction("Rewriting kernel using BASIC", 5 * 1000, null));
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
    public Processor[] getProcessors(String filter) throws ProductionException {
        // Return some dummy processors
        return new Processor[]{
                new Processor("pc1", "MERIS IOP Case2R",
                              "",
                              "beam-meris-case2r",
                              new String[]{"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                new Processor("pc2", "MERIS IOP QAA",
                              "",
                              "beam-meris-qaa",
                              new String[]{"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
                new Processor("pc3", "Band Maths",
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
        String outputFileName = getOutputFile(productionRequest, productionType);
        String inputProductSetId = getProductionParameter(productionRequest, "inputProductSetId");
        String productionName = MessageFormat.format("Producing file ''{0}'' from ''{1}'' using workflow ''{2}''",
                                                     outputFileName,
                                                     inputProductSetId,
                                                     productionType);

        long secondsToRun = (int) (10 + 20 * Math.random()); // 10...30 seconds
        File downloadDir = new File(System.getProperty("user.home"), ".calvalus/test");
        TestProduction production = new TestProduction(productionName, secondsToRun * 1000,
                                                       new File(downloadDir, outputFileName).getPath());
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

    private TestProduction getProduction(String productionId) {
        for (TestProduction production : productionList) {
            if (productionId.equals(production.getId())) {
                return production;
            }
        }
        return null;
    }

    public static String getProductionParameter(ProductionRequest productionRequest, String name) {
        for (ProductionParameter parameter : productionRequest.getProductionParameters()) {
            if (name.equals(parameter.getName())) {
                return parameter.getValue();
            }
        }
        return null;
    }

    private String getOutputFile(ProductionRequest productionRequest, String productionType) {
        return getProductionParameter(productionRequest, "outputFileName")
                .replace("${user}", System.getProperty("user.name", "Mrs Dummy"))
                .replace("${type}", productionType)
                .replace("${num}", (++counter) + "");
    }

}
