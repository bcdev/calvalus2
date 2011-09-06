package com.bc.calvalus.production.local;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.inventory.ProductSet;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.*;
import com.bc.calvalus.production.store.CsvProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

/**
 * Factory for production service that operates locally.
 */
public class LocalProductionServiceFactory implements ProductionServiceFactory {
    @Override
    public ProductionService create(Map<String, String> serviceConfiguration,
                                    File localContextDir,
                                    File localStagingDir) throws ProductionException, IOException {

        InventoryService inventoryService = new LocalInventoryService(
                // Return some dummy product sets
                new ProductSet("MER_RR__1P/r03", asDate("2004-01-01"), asDate("2009-12-31")),
                new ProductSet("MER_RR__1P/r03/2004", asDate("2004-01-01"), asDate("2004-12-31")),
                new ProductSet("MER_RR__1P/r03/2005", asDate("2005-01-01"), asDate("2005-12-31")),
                new ProductSet("MER_RR__1P/r03/2006", asDate("2006-01-01"), asDate("2006-12-31")),
                new ProductSet("MER_RR__1P/r03/2007", asDate("2007-01-01"), asDate("2007-12-31")),
                new ProductSet("MER_RR__1P/r03/2008", asDate("2008-01-01"), asDate("2008-12-31")),
                new ProductSet("MER_RR__1P/r03/2009", asDate("2009-01-01"), asDate("2009-12-31"))
        );

        LocalProcessingService processingService = new LocalProcessingService(
                new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                        "a=2\nb=5",
                                        "beam-meris-case2r",
                                        "1.5-SNAPSHOT",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                        "a=1\nb=4",
                                        "beam-meris-case2r",
                                        "1.4",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                        "a=2\nb=1",
                                        "beam-meris-case2r",
                                        "1.3",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc1", "MERIS IOP Case2R",
                                        "a=3\nb=5",
                                        "beam-meris-case2r",
                                        "1.3-marco3",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                        "u = 2\nv = 5",
                                        "beam-meris-qaa",
                                        "1.2-SNAPSHOT",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                        "u = 2\nv = 7",
                                        "beam-meris-qaa",
                                        "1.1.3",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")),
                new ProcessorDescriptor("pc2", "MERIS IOP QAA",
                                        "u = 1\nv = 2",
                                        "beam-meris-qaa",
                                        "1.0.1",
                                        new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                        new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))
        );
        SimpleStagingService stagingService = new SimpleStagingService(localStagingDir, 1);
        CsvProductionStore productionStore = new CsvProductionStore(processingService,
                                                                    new File(localContextDir, "test-productions.csv"));
        ProductionServiceImpl productionService = new ProductionServiceImpl(inventoryService,
                                                                            processingService,
                                                                            stagingService,
                                                                            productionStore,
                                                                            new DummyProductionType(processingService, stagingService));

        if (productionStore.getProductions().length == 0) {
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

    private static Date asDate(String dateString) {
        try {
            return ProductionRequest.getDateFormat().parse(dateString);
        } catch (ParseException ignore) {
            return null;
        }
    }

}
