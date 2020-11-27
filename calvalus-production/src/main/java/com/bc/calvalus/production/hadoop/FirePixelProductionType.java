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

import com.bc.calvalus.commons.Workflow;
import com.bc.calvalus.inventory.FileSystemService;
import com.bc.calvalus.processing.JobConfigNames;
import com.bc.calvalus.processing.fire.format.CommonUtils;
import com.bc.calvalus.processing.fire.format.SensorStrategy;
import com.bc.calvalus.processing.fire.format.WorkflowConfig;
import com.bc.calvalus.processing.hadoop.HadoopProcessingService;
import com.bc.calvalus.production.Production;
import com.bc.calvalus.production.ProductionException;
import com.bc.calvalus.production.ProductionRequest;
import com.bc.calvalus.production.ProductionType;
import com.bc.calvalus.staging.Staging;
import com.bc.calvalus.staging.StagingService;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;

/**
 * The production type used for formatting the BA data to the pixel format.
 *
 * @author thomas
 */
public class FirePixelProductionType extends HadoopProductionType {

    public static class Spi extends HadoopProductionType.Spi {

        @Override
        public ProductionType create(FileSystemService fileSystemService, HadoopProcessingService processing, StagingService staging) {
            return new FirePixelProductionType(fileSystemService, processing, staging);

        }
    }

    FirePixelProductionType(FileSystemService fileSystemService, HadoopProcessingService processingService,
                            StagingService stagingService) {
        super("Fire-Pixel-Formatting", fileSystemService, processingService, stagingService);
    }

    @Override
    public Production createProduction(ProductionRequest productionRequest) throws ProductionException {
        final String productionId = Production.createId(productionRequest.getProductionType());
        String year = productionRequest.getString("calvalus.year");
        String month = productionRequest.getString("calvalus.month");
        String area = productionRequest.getString("calvalus.area");
        String defaultProductionName = String.format("Fire Pixel Formatting %s/%s", year, month);
        final String productionName = productionRequest.getProductionName(defaultProductionName);

        Configuration jobConfig = createJobConfig(productionRequest);
        String outputPath = getOutputPath(productionRequest, productionId, "Fire-Pixel-Formatting");
        jobConfig.set(JobConfigNames.CALVALUS_OUTPUT_DIR, outputPath);

        ProcessorProductionRequest processorProductionRequest = new ProcessorProductionRequest(productionRequest);
        setDefaultProcessorParameters(processorProductionRequest, jobConfig);
        setRequestParameters(productionRequest, jobConfig);
        processorProductionRequest.configureProcessor(jobConfig);

        SensorStrategy strategy = CommonUtils.getStrategy(jobConfig);

        WorkflowConfig workflowConfig = new WorkflowConfig();
        workflowConfig.area = area;
        workflowConfig.jobConfig = jobConfig;
        workflowConfig.outputDir = outputPath;
        workflowConfig.processingService = getProcessingService();
        workflowConfig.productionName = productionName;
        workflowConfig.year = year;
        workflowConfig.month = month;
        workflowConfig.userName = productionRequest.getUserName();
        Workflow formattingWorkflow = strategy.getPixelFormattingWorkflow(workflowConfig);

        String stagingDir = productionRequest.getStagingDirectory(productionId);
        return new Production(productionId,
                productionName,
                null, // no dedicated output directory
                stagingDir,
                false,
                productionRequest,
                formattingWorkflow);
    }

    @Override
    protected Staging createUnsubmittedStaging(Production production) {
        throw new NotImplementedException("Staging currently not implemented for fire-cci BA pixel formatting.");
    }
}
