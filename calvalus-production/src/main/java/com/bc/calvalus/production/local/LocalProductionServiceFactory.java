/*
 * Copyright (C) 2013 Brockmann Consult GmbH (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, see http://www.gnu.org/licenses/
 */

package com.bc.calvalus.production.local;

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionService;
import com.bc.calvalus.production.ProductionServiceFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.production.store.SqlProductionStore;
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

        InventoryService inventoryService = new LocalInventoryService();

        ProcessorDescriptor case2r = new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.5", "a=2\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML",
                                                                                              "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG",
                                                                                              "1.0"));
        case2r.setParameterDescriptors(
                new ProcessorDescriptor.ParameterDescriptor("doSmile", "boolean", "Correct smile effect", "true", null),
                new ProcessorDescriptor.ParameterDescriptor("doRadiometric", "boolean", "Correct radiometric effect", "false", null),
                new ProcessorDescriptor.ParameterDescriptor("validExpressionCHL", "string",
                                                            "Valid expression additionally applied",
                                                            "!l1p_flags.CC_LAND and !l1p_flags.CC_MIXEDPIXEL and\n" +
                                                                    "!l1p_flags.CC_CLOUD and !result_flags.CHL_OUT", null),
                new ProcessorDescriptor.ParameterDescriptor("idepixAlgo", "string", "Idepix cloud algorithm", "GlobAlbedo",
                                                            new String[]{"CoastColour", "GlobAlbedo", "QWG"}),
                new ProcessorDescriptor.ParameterDescriptor("correctBands", "stringArray", "Which bands should be corrected",
                                                            "490",
                                                            new String[]{"412", "442", "490", "510", "560", "620", "665"}),
                new ProcessorDescriptor.ParameterDescriptor("outputBands", "stringArray", "Which bands should be in the output",
                                                            "490, 510, 560",
                                                            new String[]{"412", "442", "490", "510", "560", "620", "665"})

        );

        LocalProcessingService processingService = new LocalProcessingService(
                new BundleDescriptor("beam-meris-case2r", "1.5",
                                     case2r,
                                     new ProcessorDescriptor("pc1-1", "MERIS Glint", "1.2-SNAPSHOT", "a=2\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML",
                                                                                              "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG",
                                                                                              "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.4",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.4", "a=1\nb=4",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML",
                                                                                              "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG",
                                                                                              "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.3",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.3", "a=2\nb=1",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML",
                                                                                              "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG",
                                                                                              "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.3-marco3",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.3-marco3", "a=3\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.2-SNAPSHOT",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.2-SNAPSHOT", "u = 2\nv = 5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.1.3",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.1.3",
                                                             "u = 2\nv = 7",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.0.1",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.0.1",
                                                             "u = 1\nv = 2",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG_ML", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0")))
        );
        SimpleStagingService stagingService = new SimpleStagingService(localStagingDir, 1);


        // todo - get the database connect info from configuration
        String dbName = "test-productions";
        File databaseFile = new File(localContextDir, dbName);
        File databaseLogFile = new File(localContextDir, dbName + ".log");

        ProductionStore productionStore = SqlProductionStore.create(processingService,
                                                                    "org.hsqldb.jdbcDriver",
                                                                    "jdbc:hsqldb:file:" + databaseFile.getPath(), "SA", "",
                                                                    !databaseLogFile.exists());
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
