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
        final String jobName = productionRequest.getString("calvalus.hadoop.mapred.job.name",
                                                           productionRequest.getProductionType());
        final String productionId = Production.createId(jobName);
        final String productionName = productionRequest.getProdcutionName(createProductionName(productionRequest));

        HadoopWorkflowItem workflowItem = createWorkflowItem(productionId, productionRequest);

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
        return String.format("Level 2 formatting using input path '%s' and output format'%s'",
                             productionRequest.getString("inputPath"),
                             productionRequest.getString("outputFormat", "NetCDF"));
    }

    HadoopWorkflowItem createWorkflowItem(String productionId,
                                          ProductionRequest productionRequest) throws ProductionException {

        Configuration l2JobConfig = createJobConfig(productionRequest);

        String[] inputFiles = L2ProductionType.getInputFiles(getInventoryService(), productionRequest);
        String outputDir = getOutputPath(productionRequest, productionId, "");
        String outputFormat = productionRequest.getString("outputFormat", "NetCDF");

        String processorName = productionRequest.getString("processorName", null);
        if (processorName != null) {
            String processorParameters = productionRequest.getString("processorParameters", "<parameters/>");
            l2JobConfig.set(JobConfigNames.CALVALUS_L2_OPERATOR, processorName);
            l2JobConfig.set(JobConfigNames.CALVALUS_L2_PARAMETERS, processorParameters);
        }

        l2JobConfig.set(JobConfigNames.CALVALUS_INPUT, StringUtils.join(inputFiles, ","));

        String processorBundleName = productionRequest.getString("processorBundleName", null);
        String processorBundleVersion = productionRequest.getString("processorBundleVersion", null);
        if (processorBundleName != null && processorBundleVersion != null) {
            String processorBundle = String.format("%s-%s",
                                                   processorBundleName,
                                                   processorBundleVersion);
            l2JobConfig.set(JobConfigNames.CALVALUS_L2_BUNDLE, processorBundle);
        }

        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputDir);
        l2JobConfig.set(JobConfigNames.CALVALUS_OUTPUT_FORMAT, outputFormat);

        return new L2FormattingWorkflowItem(getProcessingService(), productionId, l2JobConfig);
    }

}
