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
import java.io.IOException;
import java.util.Map;

/**
 * Factory for production service that operates locally.
 */
public class LocalProductionServiceFactory implements ProductionServiceFactory {
    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    File localContextDir,
                                    File localStagingDir) throws ProductionException, IOException {

        LocalProcessingService processingService = new LocalProcessingService();
        SimpleStagingService stagingService = new SimpleStagingService(localStagingDir, 1);
        ProductionServiceImpl productionService = new ProductionServiceImpl(processingService,
                                                                            stagingService,
                                                                            new SimpleProductionStore(processingService.getJobIdFormat(),
                                                                                                      new File(localContextDir, "test-productions.csv")),
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
                                                "a=2\nb=5",
                                                "beam-meris-case2r",
                                                "1.5-SNAPSHOT"),
                        new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                                "a=1\nb=4",
                                                "beam-meris-case2r",
                                                "1.4"),
                        new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                                "a=2\nb=1",
                                                "beam-meris-case2r",
                                                "1.3"),
                        new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                                "a=3\nb=5",
                                                "beam-meris-case2r",
                                                "1.3-marco3"),


                        new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                                "u = 2\nv = 5",
                                                "beam-meris-qaa",
                                                "1.2-SNAPSHOT"),
                        new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                                "u = 2\nv = 7",
                                                "beam-meris-qaa",
                                                "1.1.3"),
                        new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                                "u = 1\nv = 2" ,
                                                "beam-meris-qaa",
                                                "1.0.1"),

                        new ProcessorDescriptor("pc3", "Band Maths",
                                                "x = 0.988\ny = 0.113\nz = 0.324",
                                                "beam-gpf",
                                                "4.8"),
                };
            }
        };

        if (new SimpleProductionStore(processingService.getJobIdFormat(),
                                      new File("test-productions.csv")).getProductions().length == 0) {
            productionService.orderProduction(new ProductionRequest("test", "ewa",
                                                                    "name", "Formatting all hard drives",
                                                                    "user", "martin",
                                                                    "autoStaging", "true"));
            productionService.orderProduction(new ProductionRequest("test", "ewa",
                                                                    "name", "Drying CD slots",
                                                                    "user", "marcoz",
                                                                    "autoStaging", "true"));
            productionService.orderProduction(new ProductionRequest("test", "ewa",
                                                                    "name", "Rewriting kernel using BASIC",
                                                                    "user", "norman",
                                                                    "autoStaging", "false"));
        }

        return productionService;

    }

}
