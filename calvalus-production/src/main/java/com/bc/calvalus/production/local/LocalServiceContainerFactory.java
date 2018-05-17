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

import com.bc.calvalus.inventory.AbstractFileSystemService;
import com.bc.calvalus.inventory.ColorPaletteService;
import com.bc.calvalus.inventory.DefaultColorPaletteService;
import com.bc.calvalus.inventory.DefaultInventoryService;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.AggregatorDescriptor;
import com.bc.calvalus.processing.BundleDescriptor;
import com.bc.calvalus.processing.ProcessorDescriptor;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ServiceContainer;
import com.bc.calvalus.production.ServiceContainerFactory;
import com.bc.calvalus.production.ProductionServiceImpl;
import com.bc.calvalus.production.store.ProductionStore;
import com.bc.calvalus.production.store.SqlProductionStore;
import com.bc.calvalus.staging.SimpleStagingService;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Factory for production service that operates locally.
 */
public class LocalServiceContainerFactory implements ServiceContainerFactory {

    @Override
    public ServiceContainer create(Map<String, String> serviceConfiguration,
                                   File localContextDir,
                                   File localStagingDir) throws ProductionException, IOException {

        AbstractFileSystemService fileSystemService = new LocalFileSystemService();
        InventoryService inventoryService = new DefaultInventoryService(fileSystemService, "eodata");
        ColorPaletteService colorPaletteService = new DefaultColorPaletteService(fileSystemService, "auxiliary");

        ProcessorDescriptor case2r = new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.5", "a=2\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"));
        case2r.setParameterDescriptors(
                new ProcessorDescriptor.ParameterDescriptor("doSmile", "boolean", "Correct smile effect", "true"),
                new ProcessorDescriptor.ParameterDescriptor("doRadiometric", "boolean", "Correct radiometric effect", "false"),
                new ProcessorDescriptor.ParameterDescriptor("validExpressionCHL", "string",
                                                            "Valid expression additionally applied",
                                                            "!l1p_flags.CC_LAND and !l1p_flags.CC_MIXEDPIXEL and\n" +
                                                            "!l1p_flags.CC_CLOUD and !result_flags.CHL_OUT"),
                new ProcessorDescriptor.ParameterDescriptor("idepixAlgo", "string", "Idepix cloud algorithm", "GlobAlbedo",
                                                            new String[]{"CoastColour", "GlobAlbedo", "QWG"}),
                new ProcessorDescriptor.ParameterDescriptor("correctBands", "stringArray", "Which bands should be corrected",
                                                            "490",
                                                            new String[]{"412", "442", "490", "510", "560", "620", "665"}),
                new ProcessorDescriptor.ParameterDescriptor("outputBands", "stringArray", "Which bands should be in the output",
                                                            "490, 510, 560",
                                                            new String[]{"412", "442", "490", "510", "560", "620", "665"})

        );

        BundleDescriptor aggregatorBundle = new BundleDescriptor("aggregator", "1.0", "/software/system");
        AggregatorDescriptor avg = new AggregatorDescriptor("AVG");
        avg.setParameterDescriptors(
                new ProcessorDescriptor.ParameterDescriptor("varName", "variable", "The source band used for aggregation."),
                new ProcessorDescriptor.ParameterDescriptor("targetName", "string", "The name prefix for the resulting bands. If empty, the source band name is used."),
                new ProcessorDescriptor.ParameterDescriptor("weightCoeff", "float", "The number of spatial observations to the power of this value \n" +
                                                 "will define the value for weighting the sums. Zero means observation count weighting is disabled.", "0.0"),
                new ProcessorDescriptor.ParameterDescriptor("outputCounts", "boolean", "If true, the result will include the count of all valid values.", "false"),
                new ProcessorDescriptor.ParameterDescriptor("outputSums", "boolean", "If true, the result will include the sum of all values.", "false")
        );
        AggregatorDescriptor onMaxSet = new AggregatorDescriptor("ON_MAX_SET");
        onMaxSet.setParameterDescriptors(
                new ProcessorDescriptor.ParameterDescriptor("onMaxVarName", "variable", "If this band reaches its maximum the values of the source bands are taken."),
                new ProcessorDescriptor.ParameterDescriptor("targetName", "string", "The name prefix for the resulting bands. If empty, the source band name is used."),
                new ProcessorDescriptor.ParameterDescriptor("setVarNames", "variableArray", "The source bands used for aggregation when maximum band reaches its maximum.")
        );
        aggregatorBundle.setAggregatorDescriptors(avg, onMaxSet);

        LocalProcessingService processingService = new LocalProcessingService(
                new BundleDescriptor("beam-meris-case2r", "1.5", "/software/user/olga",
                                     case2r,
                                     new ProcessorDescriptor("pc1-1", "MERIS Glint", "1.2-SNAPSHOT", "a=2\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.4", "/software/user/olga",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.4", "a=1\nb=4",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.3", "/software/system",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.3", "a=2\nb=1",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-case2r", "1.3-marco3", "/software/user/martin",
                                     new ProcessorDescriptor("pc1", "MERIS IOP Case2R", "1.3-marco3", "a=3\nb=5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.2-SNAPSHOT", "/software/system",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.2-SNAPSHOT", "u = 2\nv = 5",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.1.3", "/software/system",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.1.3",
                                                             "u = 2\nv = 7",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                new BundleDescriptor("beam-meris-qaa", "1.0.1", "/software/system",
                                     new ProcessorDescriptor("pc2", "MERIS IOP QAA", "1.0.1",
                                                             "u = 1\nv = 2",
                                                             new ProcessorDescriptor.Variable("chl_conc", "AVG", "0.5"),
                                                             new ProcessorDescriptor.Variable("tsm_conc", "AVG", "1.0"))),
                aggregatorBundle
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
        ProductionServiceImpl productionService = new ProductionServiceImpl(fileSystemService,
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

        return new ServiceContainer(productionService, fileSystemService, inventoryService, colorPaletteService, new Configuration());

    }

}
