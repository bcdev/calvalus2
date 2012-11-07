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

import com.bc.calvalus.inventory.InventoryService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.processing.hadoop.HadoopWorkflowItem;
import com.bc.calvalus.processing.l2.L2FormattingWorkflowItem;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.esa.beam.util.StringUtils;

/**
 * A production type used for formatting one or more Level-2 products.
 *
 * @author MarcoZ
 * @author Norman
 */
public class L2FProductionType extends HadoopProductionType {

    static final String NAME = "L2F";

    public L2FProductionType(InventoryService inventoryService, HadoopProcessingService processingService,
                             StagingService stagingService) {
        super(NAME, inventoryService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        final String productionName = productionRequest.getProdcutionName(createProductionName(productionRequest));

        HadoopWorkflowItem workflowItem = createWorkflowItem(productionId, productionName, productionRequest);

        // todo - if autoStaging=true, create sequential workflow and add staging job
        String stagingDir = productionRequest.getStagingDirectory(productionId);
        boolean autoStaging = productionRequest.isAutoStaging();
        return new Production(productionId,
                              productionName,
                              workflowItem.getOutputDir(),
                              stagingDir,
                              autoStaging,
                              productionRequest,
                              workflowItem);
    }

    // TODO, at the moment no staging implemented
    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for product validation.");
    }

    static String createProductionName(ProductionRequest productionRequest) throws ProductionException {
        String prefix = String.format("Level 2 Format %s ", productionRequest.getString("outputFormat", "NetCDF"));
        return L2ProductionType.createProductionName(prefix, productionRequest);
    }

    HadoopWorkflowItem createWorkflowItem(String productionId,
                                          String productionName,
                                          ProductionRequest productionRequest) throws ProductionException {

        Configuration jobConfig = createJobConfig(productionRequest);
        setDefaultProcessorParameters(jobConfig, new ProcessorProductionRequest(productionRequest));
        setRequestParameters(jobConfig, productionRequest);

        String[] inputFiles = L2ProductionType.getInputFiles(getInventoryService(), productionRequest);
        jobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));

        Geometry regionGeometry = productionRequest.getRegionGeometry(null);
        if (regionGeometry != null) {
            jobConfig.set(JobConfigNames.CALVALUS_REGION_GEOMETRY, regionGeometry.toString());
        }

        String outputDir = getOutputPath(productionRequest, productionId, "");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);

        String outputFormat = productionRequest.getString("outputFormat", productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_FORMAT,"NetCDF"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        // is in fact dependent on the outputFormat TODO unify
        String outputCompression = productionRequest.getString("outputCompression", productionRequest.getString(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION,"gz"));
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_COMPRESSION, outputCompression);

        return new L2FormattingWorkflowItem(getProcessingService(), productionName, jobConfig);
    }

}
