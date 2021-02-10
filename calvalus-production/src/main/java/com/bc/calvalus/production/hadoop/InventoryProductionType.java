/*
 * Copyright (C) 2011 Brockmann Consult GmbH (info@brockmann-consult.de)
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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.productinventory.ProductInventoryWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.locationtech.jts.geom.Geometry;
import org.apache.hadoop.conf.Configuration;
import org.esa.snap.core.util.StringUtils;

import java.util.List;

/**
 * A production type used for checking one or more products for validity and generating an inventory.
 *
 * @author MarcoZ
 */
public class InventoryProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new InventoryProductionType(fileSystemService, processing, staging);
        }
    }

    InventoryProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                                   StagingService stagingService) {
        super("Inventory", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Inventory ", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        WorkflowItem workflowItem = createWorkflowItem(productionId, productionName, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        //boolean autoStaging = productionRequest.isAutoStaging(); //TODO
        boolean autoStaging = false;
        return new Production(productionId,
                              productionName,
                              "",
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new UnsupportedOperationException("Staging currently not implemented for product inventory.");
    }

    WorkflowItem createWorkflowItem(String productionId,
                                    String productionName,
                                    ProductionRequest productionRequest) throws ProductionException {


        Configuration jobConfig = createJobConfig(productionRequest);
        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        List<DateRange> dateRanges = productionRequest.getDateRanges();
        //jobConfig.set(JobConfigNames.CALVALUS_INPUT_PATH_PATTERNS, productionRequest.getString("inputPath"));
        setInputLocationParameters(productionRequest, jobConfig);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_REGION_NAME, productionRequest.getRegionName());
        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DATE_RANGES, StringUtils.join(dateRanges, ","));

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        Geometry regionGeom = productionRequest.getRegionGeometry(null);
        jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeom != null ? regionGeom.toString() : "");

        return new ProductInventoryWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                productionName, jobConfig);
    }

}
