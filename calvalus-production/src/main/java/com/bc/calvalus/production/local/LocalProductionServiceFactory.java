package com.bc.calvalus.production.local;

import com.bc.calvalus.catalogue.ProductSet;
import com.bc.calvalus.production.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.SimpleProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;

import java.io.File;
import java.util.Map;

/**
 * Factory for the {@link com.bc.calvalus.production.test.TestProductionService}.
 */
public class LocalProductionServiceFactory implements ProductionServiceFactory {
    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    String localStagingDir) throws ProductionException {

        LocalProcessingService processingService = new LocalProcessingService();
        SimpleStagingService stagingService = new SimpleStagingService(localStagingDir, 1);
        ProductionServiceImpl productionService = new ProductionServiceImpl(processingService,
                                                                            stagingService,
                                                                            new SimpleProductionStore(processingService.getJobIdFormat(),
                                                                                                      new File("test-productions.csv")),
                                                                            new DummyProductionType(processingService, stagingService)) {
            @Override
            public ProductSet[] getProductSets(String filter) throws ProductionException {
                // Return some dummy product sets
                return new ProductSet[]{
                        new ProductSet("MER_RR__1P/r03", "MERIS-L1B", "MERIS RR 2004-2009"),
                        new ProductSet("MER_RR__1P/r03/2004", "MERIS-L1B", "MERIS RR 2004"),
                        new ProductSet("MER_RR__1P/r03/2005", "MERIS-L1B", "MERIS RR 2005"),
                        new ProductSet("MER_RR__1P/r03/2006", "MERIS-L1B", "MERIS RR 2006"),
                        new ProductSet("MER_RR__1P/r03/2007", "MERIS-L1B", "MERIS RR 2007"),
                        new ProductSet("MER_RR__1P/r03/2008", "MERIS-L1B", "MERIS RR 2008"),
                        new ProductSet("MER_RR__1P/r03/2009", "MERIS-L1B", "MERIS RR 2009"),
                };
            }

            @Override
            public ProcessorDescriptor[] getProcessors(String filter) throws ProductionException {
                // Return some dummy processors
                return new ProcessorDescriptor[]{
                        new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                                "",
                                                "beam-meris-case2r",
                                                new String[]{"1.5-SNAPSHOT", "1.4", "1.3", "1.3-marco3"}),
                        new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                                "",
                                                "beam-meris-qaa",
                                                new String[]{"1.2-SNAPSHOT", "1.1.3", "1.0.1"}),
                        new ProcessorDescriptor("pc3", "Band Maths",
                                                "",
                                                "beam-gpf",
                                                new String[]{"4.8"}),
                };
            }
        };

        if (new SimpleProductionStore(processingService.getJobIdFormat(),
                                      new File("test-productions.csv")).getProductions().length == 0) {
            productionService.orderProduction(new ProductionRequest("test",
                                                                    "name", "Formatting all hard drives",
                                                                    "user", "martin",
                                                                    "autoStaging", "true"));
            productionService.orderProduction(new ProductionRequest("test",
                                                                    "name", "Drying CD slots",
                                                                    "user", "marcoz",
                                                                    "autoStaging", "true"));
            productionService.orderProduction(new ProductionRequest("test",
                                                                    "name", "Rewriting kernel using BASIC",
                                                                    "user", "norman",
                                                                    "autoStaging", "false"));
        }

        productionService.startStatusObserver(2000);
        return productionService;

    }

}
