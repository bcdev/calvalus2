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

package com.bc.calvalus.production.hadoop;

import com.bc.calvalus.commons.DateRange;
import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.commons.WorkflowItem;
import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.l3.CellL3ProcessorWorkflowItem;
import com.bc.calvalus.processing.l3.CellProcessorWorkflowItem;
import com.bc.calvalus.processing.l3.L3FormatWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

/**
 * A production type used for transforming Level-3 products into a new Level-3 product
 * without changing the spatial or timely resolution (NO new aggregation)
 *
 * @author MarcoZ
 */
public class CellProcessorProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(InventoryService inventory, HadoopProcessingService processing, StagingService staging) {
            return new CellProcessorProductionType(inventory, processing, staging);
        }
    }

    CellProcessorProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                                StagingService stagingService) {
        super("Cell", inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {

        final String productionId = Production.createId(productionRequest.getProductionType());
        String defaultProductionName = createProductionName("Cell Processing", productionRequest);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        String outputDirParts = getOutputPath(productionRequest, productionId, "");
        String outputDirProducts = getOutputPath(productionRequest, productionId, "-output");

        jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, productionRequest.getString("inputPath"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirParts);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(
                JobConfigNames.CALVALUS_OUTPUT_FORMAT, null));

        WorkflowItem workflowItem = new CellProcessorWorkflowItem(getProcessingService(), productionRequest.getUserName(),
                                                                  productionName, jobConfig);
        if (outputFormat != null) {
            jobConfig = createJobConfig(productionRequest);
            setDefaultProcessorParameters(processorProductionRequest, jobConfig);
            setRequestParameters(productionRequest, jobConfig);
            processorProductionRequest.configureProcessor(jobConfig);

            jobConfig.set(JobConfigNames.CALVALUS_INPUT_DIR, outputDirParts);
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDirProducts);

            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);
            // is in fact dependent on the outputFormat TODO unify
            String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(
                    JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, "gz"));
            jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

            WorkflowItem formatItem = new L3FormatWorkflowItem(getProcessingService(),
                                                               productionRequest.getUserName(),
                                                               productionName + " Format",
                                                               jobConfig);
            workflowItem = new Workflow.Sequential(workflowItem, formatItem);
        }

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              outputDirProducts,
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for L3Proc.");
    }
}
